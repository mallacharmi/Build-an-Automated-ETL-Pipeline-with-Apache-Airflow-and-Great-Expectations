"""
Analytical Store Loader module.
Loads cleansed and aggregated Silver Parquet data into SQLite serving database
(table: daily_country_sales) with idempotency guarantees.
"""

import logging
import os
import sqlite3
import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )


def load_to_sqlite(
    silver_dir: str,
    db_path: str = "data/analytics.db",
    table_name: str = "daily_country_sales",
) -> None:
    """
    Read partitioned Silver Parquet dataset and load into an SQLite table.

    Args:
        silver_dir (str): Directory containing partitioned Silver Parquet data.
        db_path (str): File path for the SQLite database.
        table_name (str): Destination table name (defaults to 'daily_country_sales').

    Raises:
        FileNotFoundError: If silver_dir does not exist.
        Exception: If database loading or connection fails.
    """
    logger.info("Starting load to analytical store task.")
    logger.info("Source Silver directory: %s", silver_dir)
    logger.info("Destination SQLite database: %s", db_path)
    logger.info("Destination table: %s", table_name)

    if not os.path.exists(silver_dir):
        logger.error("Silver directory not found: %s", silver_dir)
        raise FileNotFoundError(f"Silver directory not found: {silver_dir}")

    # Read the full Silver Parquet dataset
    logger.info("Reading Silver Parquet data...")
    df = pd.read_parquet(silver_dir, engine="pyarrow")
    logger.info("Read %d aggregated records from Silver Parquet layer.", len(df))

    # Ensure target database directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Establish SQLite connection
    conn = None
    try:
        logger.info("Connecting to SQLite database: %s", db_path)
        conn = sqlite3.connect(db_path)

        # Idempotent write: if_exists='replace' replaces table on every run
        logger.info("Writing dataframe to SQLite table '%s' with if_exists='replace'...", table_name)
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Verify loaded rows
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        loaded_count = cursor.fetchone()[0]
        logger.info("Successfully verified %d rows in table '%s'.", loaded_count, table_name)

    except Exception as exc:
        logger.error("Failed to load data into SQLite database: %s", exc, exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("SQLite connection closed gracefully.")

    logger.info("Analytical store loading task completed successfully.")


if __name__ == "__main__":
    import sys
    s_dir = sys.argv[1] if len(sys.argv) > 1 else "data/silver"
    db = sys.argv[2] if len(sys.argv) > 2 else "data/analytics.db"
    tbl = sys.argv[3] if len(sys.argv) > 3 else "daily_country_sales"
    load_to_sqlite(s_dir, db, tbl)
