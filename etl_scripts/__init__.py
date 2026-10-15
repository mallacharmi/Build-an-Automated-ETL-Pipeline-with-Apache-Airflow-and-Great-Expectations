"""
ETL Scripts Package for E-commerce Analytics Pipeline.
Provides modular components for:
- Raw data ingestion (raw_downloader)
- Bronze layer processing and partitioning (bronze_processor)
- Silver layer cleansing and aggregation (silver_transformer)
- SQLite analytical store loading (analytics_loader)
- Great Expectations validation circuit breaker (validator)
"""

from .raw_downloader import download_raw_data
from .bronze_processor import process_to_bronze
from .silver_transformer import transform_to_silver
from .analytics_loader import load_to_sqlite
from .validator import validate_bronze_layer, validate_silver_layer

__all__ = [
    "download_raw_data",
    "process_to_bronze",
    "transform_to_silver",
    "load_to_sqlite",
    "validate_bronze_layer",
    "validate_silver_layer",
]
