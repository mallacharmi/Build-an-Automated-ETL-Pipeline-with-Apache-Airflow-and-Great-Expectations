"""
Apache Airflow DAG for Orchestrating the E-commerce Analytics ETL Pipeline.

Flow:
download_raw_data -> bronze_layer_processing -> bronze_data_validation ->
silver_layer_transformation -> silver_data_validation -> load_to_analytical_store
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure root directory is on the Python path for modular imports
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

# Import modular ETL components
from etl_scripts.raw_downloader import download_raw_data
from etl_scripts.bronze_processor import process_to_bronze
from etl_scripts.silver_transformer import transform_to_silver
from etl_scripts.analytics_loader import load_to_sqlite
from etl_scripts.validator import validate_bronze_layer, validate_silver_layer

# Environment configurations with container defaults
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_CSV_PATH = os.path.join(DATA_DIR, "raw", "ecommerce_data.csv")
BRONZE_DIR_PATH = os.path.join(DATA_DIR, "bronze")
SILVER_DIR_PATH = os.path.join(DATA_DIR, "silver")
SQLITE_DB_PATH = os.path.join(DATA_DIR, "analytics.db")

GE_DIR = os.environ.get("GREAT_EXPECTATIONS_DIR", "/opt/airflow/great_expectations")
BRONZE_SUITE_PATH = os.path.join(GE_DIR, "expectations", "bronze_expectations.json")
SILVER_SUITE_PATH = os.path.join(GE_DIR, "expectations", "silver_expectations.json")

DATASET_URL = os.environ.get(
    "DATASET_URL",
    "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/by-day/2010-12-01.csv",
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=default_args,
    description="Automated Medallion Architecture ETL with Airflow and Great Expectations",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "great_expectations", "medallion"],
) as dag:

    # Task 1: Ingest raw data from external source
    download_raw_data_task = PythonOperator(
        task_id="download_raw_data",
        python_callable=download_raw_data,
        op_kwargs={
            "url": DATASET_URL,
            "dest_path": RAW_CSV_PATH,
        },
    )

    # Task 2: Process raw CSV into partitioned Bronze Parquet layer
    bronze_layer_processing_task = PythonOperator(
        task_id="bronze_layer_processing",
        python_callable=process_to_bronze,
        op_kwargs={
            "input_csv": RAW_CSV_PATH,
            "output_dir": BRONZE_DIR_PATH,
        },
    )

    # Task 3: Validate Bronze layer schema and invariants via Great Expectations
    bronze_data_validation_task = PythonOperator(
        task_id="bronze_data_validation",
        python_callable=validate_bronze_layer,
        op_kwargs={
            "bronze_dir": BRONZE_DIR_PATH,
            "suite_path": BRONZE_SUITE_PATH,
        },
    )

    # Task 4: Cleans, filters, and transforms Bronze data into daily aggregated Silver Parquet layer
    silver_layer_transformation_task = PythonOperator(
        task_id="silver_layer_transformation",
        python_callable=transform_to_silver,
        op_kwargs={
            "bronze_dir": BRONZE_DIR_PATH,
            "silver_dir": SILVER_DIR_PATH,
        },
    )

    # Task 5: Validate Silver layer aggregations and uniqueness via Great Expectations
    silver_data_validation_task = PythonOperator(
        task_id="silver_data_validation",
        python_callable=validate_silver_layer,
        op_kwargs={
            "silver_dir": SILVER_DIR_PATH,
            "suite_path": SILVER_SUITE_PATH,
        },
    )

    # Task 6: Load validated Silver data into SQLite analytical query store
    load_to_analytical_store_task = PythonOperator(
        task_id="load_to_analytical_store",
        python_callable=load_to_sqlite,
        op_kwargs={
            "silver_dir": SILVER_DIR_PATH,
            "db_path": SQLITE_DB_PATH,
            "table_name": "daily_country_sales",
        },
    )

    # Strict linear task dependency definition
    (
        download_raw_data_task
        >> bronze_layer_processing_task
        >> bronze_data_validation_task
        >> silver_layer_transformation_task
        >> silver_data_validation_task
        >> load_to_analytical_store_task
    )
