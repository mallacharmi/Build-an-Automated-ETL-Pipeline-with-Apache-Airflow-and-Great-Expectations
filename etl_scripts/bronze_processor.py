"""
Bronze layer processor module.
Ingests raw CSV data, parses timestamps, extracts Year and Month partition keys,
enforces schema typing, and serializes to partitioned Apache Parquet files.
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


def process_to_bronze(input_csv: str, output_dir: str) -> None:
    """
    Process raw CSV data into a partitioned Bronze Parquet dataset.

    Args:
        input_csv (str): Path to the raw CSV file.
        output_dir (str): Destination directory for the partitioned Parquet dataset.

    Raises:
        FileNotFoundError: If the input CSV does not exist.
        ValueError: If required columns are missing from the raw data.
        Exception: If parsing or serialization fails.
    """
    logger.info("Starting Bronze layer processing task.")
    logger.info("Input CSV: %s", input_csv)
    logger.info("Output Directory: %s", output_dir)

    if not os.path.exists(input_csv):
        logger.error("Input CSV file does not exist: %s", input_csv)
        raise FileNotFoundError(f"Input raw CSV file not found: {input_csv}")

    # Read CSV supporting UTF-8 and ISO-8859-1 (standard for UCI Online Retail)
    try:
        logger.info("Reading raw CSV with UTF-8 encoding...")
        df = pd.read_csv(input_csv, encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning("UTF-8 decoding failed. Retrying with ISO-8859-1 encoding...")
        df = pd.read_csv(input_csv, encoding="ISO-8859-1")

    logger.info("Loaded raw CSV with %d rows and %d columns.", len(df), len(df.columns))

    # Support schema variations where InvoiceDate might be labeled Date
    if "InvoiceDate" not in df.columns and "Date" in df.columns:
        logger.info("Mapping 'Date' column to 'InvoiceDate'...")
        df["InvoiceDate"] = df["Date"]

    required_cols = [
        "InvoiceNo",
        "StockCode",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        error_msg = f"Raw CSV missing required columns: {missing}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Convert InvoiceDate to datetime
    logger.info("Parsing 'InvoiceDate' to datetime...")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    null_dates = df["InvoiceDate"].isna().sum()
    if null_dates > 0:
        logger.warning("Found %d rows with unparseable InvoiceDate.", null_dates)

    # Extract Year and Month for partitioning
    df["Year"] = df["InvoiceDate"].dt.year.fillna(1970).astype(int)
    df["Month"] = df["InvoiceDate"].dt.month.fillna(1).astype(int)

    # Cast Quantity to integer and UnitPrice to float to meet schema typing
    logger.info("Enforcing schema data types for Quantity and UnitPrice...")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("int64")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0).astype("float64")

    # Format CustomerID consistently (preserve string format or null)
    # Convert numeric-looking customer IDs like 17850.0 to '17850'
    def format_customer_id(val):
        if pd.isna(val) or val is None or str(val).strip() == "" or str(val).lower() == "nan":
            return None
        s = str(val).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    df["CustomerID"] = df["CustomerID"].apply(format_customer_id)

    # Cast InvoiceNo, StockCode, Country to string/object
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["StockCode"] = df["StockCode"].astype(str)
    df["Country"] = df["Country"].astype(str)

    # Idempotency: Remove existing destination directory before writing
    if os.path.exists(output_dir):
        logger.info("Cleaning existing Bronze directory for idempotency: %s", output_dir)
        shutil.rmtree(output_dir, ignore_errors=True)

    os.makedirs(output_dir, exist_ok=True)

    # Write to Parquet partitioned by Year and Month
    logger.info(
        "Serializing %d rows to Parquet in %s partitioned by ['Year', 'Month']...",
        len(df),
        output_dir,
    )
    df.to_parquet(
        output_dir,
        partition_cols=["Year", "Month"],
        index=False,
        engine="pyarrow",
    )

    logger.info("Bronze layer processing completed successfully.")


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/ecommerce_data.csv"
    bronze_path = sys.argv[2] if len(sys.argv) > 2 else "data/bronze"
    process_to_bronze(csv_path, bronze_path)
