"""
Silver layer transformer module.
Cleanses Bronze data according to business rules:
- Imputes missing CustomerIDs with 'UNKNOWN'
- Filters out non-positive unit prices and negative quantities
- Derives TotalPrice = Quantity * UnitPrice
- Aggregates daily sales by Day-level InvoiceDate and Country (DailyTotalSales)
- Partitions by Year and Month into Apache Parquet format
"""

import logging
import os
import shutil
import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )


def impute_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace missing or null CustomerID values with the string literal 'UNKNOWN'.
    """
    df = df.copy()
    if "CustomerID" in df.columns:
        df["CustomerID"] = df["CustomerID"].fillna("UNKNOWN")
        # Also handle empty strings, 'nan', 'None'
        df["CustomerID"] = df["CustomerID"].apply(
            lambda x: "UNKNOWN" if pd.isna(x) or str(x).strip() in ("", "nan", "None", "NaN") else str(x).strip()
        )
    return df


def filter_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out records where Quantity < 0 or UnitPrice <= 0.
    Retains only records with Quantity >= 0 and UnitPrice > 0.
    """
    df = df.copy()
    initial_count = len(df)
    filtered_df = df[(df["Quantity"] >= 0) & (df["UnitPrice"] > 0)]
    dropped_count = initial_count - len(filtered_df)
    logger.info("Filtered invalid records: dropped %d rows (retained %d rows).", dropped_count, len(filtered_df))
    return filtered_df


def calculate_total_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mathematically calculate TotalPrice = Quantity * UnitPrice.
    """
    df = df.copy()
    df["TotalPrice"] = (df["Quantity"] * df["UnitPrice"]).round(2)
    return df


def aggregate_daily_country_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Truncate InvoiceDate to day level (YYYY-MM-DD) and group by (InvoiceDate, Country),
    summing TotalPrice into DailyTotalSales.
    Extracts Year and Month for physical partitioning.
    """
    df = df.copy()
    # Normalize or format date to YYYY-MM-DD
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"]).dt.strftime("%Y-%m-%d")

    agg_df = (
        df.groupby(["InvoiceDate", "Country"], as_index=False)["TotalPrice"]
        .sum()
        .rename(columns={"TotalPrice": "DailyTotalSales"})
    )
    agg_df["DailyTotalSales"] = agg_df["DailyTotalSales"].round(2)

    # Derive Year and Month from the day-level InvoiceDate for physical partitioning
    date_dt = pd.to_datetime(agg_df["InvoiceDate"])
    agg_df["Year"] = date_dt.dt.year.astype(int)
    agg_df["Month"] = date_dt.dt.month.astype(int)

    return agg_df


def apply_silver_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the full sequence of business transformations to a raw/bronze DataFrame:
    1. Impute CustomerID
    2. Filter out Quantity < 0 or UnitPrice <= 0
    3. Calculate TotalPrice
    4. Group by day-level InvoiceDate and Country to compute DailyTotalSales
    """
    logger.info("Applying full business transformation suite to DataFrame...")
    df_step1 = impute_customer_id(df)
    df_step2 = filter_invalid_records(df_step1)
    df_step3 = calculate_total_price(df_step2)
    df_final = aggregate_daily_country_sales(df_step3)
    logger.info("Transformation produced %d aggregated records.", len(df_final))
    return df_final


def transform_to_silver(bronze_dir: str, silver_dir: str) -> None:
    """
    Read partitioned Parquet dataset from bronze_dir, apply business rules,
    and serialize partitioned Parquet dataset to silver_dir.

    Args:
        bronze_dir (str): Path to input Bronze partitioned Parquet directory.
        silver_dir (str): Path to output Silver partitioned Parquet directory.

    Raises:
        FileNotFoundError: If bronze_dir does not exist or has no Parquet files.
        Exception: If transformation or write fails.
    """
    logger.info("Starting Silver layer transformation task.")
    logger.info("Bronze source directory: %s", bronze_dir)
    logger.info("Silver target directory: %s", silver_dir)

    if not os.path.exists(bronze_dir):
        logger.error("Bronze directory not found: %s", bronze_dir)
        raise FileNotFoundError(f"Bronze directory not found: {bronze_dir}")

    # Read the Bronze Parquet dataset (pyarrow automatically discovers partitions)
    logger.info("Reading Bronze Parquet dataset...")
    df = pd.read_parquet(bronze_dir, engine="pyarrow")
    logger.info("Loaded Bronze dataset with %d rows and %d columns.", len(df), len(df.columns))

    # Apply transformations
    silver_df = apply_silver_transformations(df)

    # Idempotency: Clean destination directory before writing
    if os.path.exists(silver_dir):
        logger.info("Cleaning existing Silver directory for idempotency: %s", silver_dir)
        shutil.rmtree(silver_dir, ignore_errors=True)

    os.makedirs(silver_dir, exist_ok=True)

    # Write aggregated result to silver_dir as partitioned Parquet
    logger.info(
        "Writing Silver dataset (%d rows) to %s partitioned by ['Year', 'Month']...",
        len(silver_df),
        silver_dir,
    )
    silver_df.to_parquet(
        silver_dir,
        partition_cols=["Year", "Month"],
        index=False,
        engine="pyarrow",
    )

    logger.info("Silver layer transformation completed successfully.")


if __name__ == "__main__":
    import sys
    b_dir = sys.argv[1] if len(sys.argv) > 1 else "data/bronze"
    s_dir = sys.argv[2] if len(sys.argv) > 2 else "data/silver"
    transform_to_silver(b_dir, s_dir)
