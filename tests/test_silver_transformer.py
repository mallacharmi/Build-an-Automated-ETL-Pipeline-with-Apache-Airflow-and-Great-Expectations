"""
Unit tests for Silver Layer Transformation logic.
Tests business rules:
1. CustomerID imputation ('UNKNOWN')
2. Filtering of negative quantities (Quantity < 0) and non-positive prices (UnitPrice <= 0)
3. TotalPrice arithmetic (Quantity * UnitPrice) and daily country aggregation (DailyTotalSales)
4. End-to-end transformation execution
"""

import os
import numpy as np
import pandas as pd
import pytest

from etl_scripts.silver_transformer import (
    impute_customer_id,
    filter_invalid_records,
    calculate_total_price,
    aggregate_daily_country_sales,
    apply_silver_transformations,
    transform_to_silver,
)


@pytest.fixture
def sample_dirty_data():
    """Provides a dirty sample dataset mimicking Bronze layer data."""
    return pd.DataFrame({
        "InvoiceNo": ["536365", "536366", "536367", "536368", "C536369", "536370"],
        "StockCode": ["85123A", "71053", "84406B", "84029G", "71053", "22728"],
        "Description": ["WHITE HANGING HEART", "WHITE METAL LANTERN", "CREAM CUPID", "KNITTED UNION FLAG", "RETURN", "ALARM CLOCK"],
        "Quantity": [6, -2, 8, 0, -5, 12],
        "InvoiceDate": [
            "2026-01-10 08:26:00",
            "2026-01-10 09:15:00",
            "2026-01-10 14:30:00",
            "2026-01-10 16:00:00",
            "2026-01-11 10:00:00",
            "2026-01-11 12:45:00",
        ],
        "UnitPrice": [2.55, 3.39, 2.75, 0.00, 4.25, 3.75],
        "CustomerID": ["17850", np.nan, "13047", None, "17850", ""],
        "Country": ["United Kingdom", "United Kingdom", "United Kingdom", "Germany", "United Kingdom", "France"],
    })


def test_customer_id_imputation(sample_dirty_data):
    """
    Test Case 1: Validate CustomerID imputation.
    Ensures null, NaN, None, and empty string CustomerID values are converted to 'UNKNOWN',
    while valid IDs are preserved.
    """
    result_df = impute_customer_id(sample_dirty_data)

    assert "CustomerID" in result_df.columns
    # Rows with index 1 (NaN), 3 (None), and 5 ('') must be imputed to 'UNKNOWN'
    assert result_df.loc[1, "CustomerID"] == "UNKNOWN"
    assert result_df.loc[3, "CustomerID"] == "UNKNOWN"
    assert result_df.loc[5, "CustomerID"] == "UNKNOWN"

    # Valid Customer IDs must be preserved
    assert result_df.loc[0, "CustomerID"] == "17850"
    assert result_df.loc[2, "CustomerID"] == "13047"
    assert result_df.loc[4, "CustomerID"] == "17850"

    # Confirm no null values remain
    assert result_df["CustomerID"].isna().sum() == 0


def test_filter_invalid_records(sample_dirty_data):
    """
    Test Case 2: Verify filtering of negative quantities and non-positive prices.
    Rows with Quantity < 0 or UnitPrice <= 0 must be discarded.
    """
    result_df = filter_invalid_records(sample_dirty_data)

    # In sample_dirty_data:
    # Row 0: Quantity=6, UnitPrice=2.55 -> KEEP
    # Row 1: Quantity=-2 (negative) -> DROP
    # Row 2: Quantity=8, UnitPrice=2.75 -> KEEP
    # Row 3: Quantity=0, UnitPrice=0.00 (UnitPrice <= 0) -> DROP
    # Row 4: Quantity=-5 (negative) -> DROP
    # Row 5: Quantity=12, UnitPrice=3.75 -> KEEP
    assert len(result_df) == 3

    # Ensure no record has Quantity < 0 or UnitPrice <= 0
    assert (result_df["Quantity"] < 0).sum() == 0
    assert (result_df["UnitPrice"] <= 0).sum() == 0

    # Ensure retained records are strictly valid
    expected_invoice_nos = ["536365", "536367", "536370"]
    assert list(result_df["InvoiceNo"]) == expected_invoice_nos


def test_total_price_arithmetic_and_aggregation():
    """
    Test Case 3: Verify TotalPrice arithmetic and daily country sales aggregation.
    TotalPrice = Quantity * UnitPrice.
    Grouping by day-level InvoiceDate and Country must calculate DailyTotalSales = sum(TotalPrice).
    """
    clean_data = pd.DataFrame({
        "InvoiceNo": ["1001", "1002", "1003", "1004"],
        "Quantity": [10, 5, 20, 2],
        "UnitPrice": [2.50, 4.00, 1.50, 10.00],
        "InvoiceDate": [
            "2026-03-15 09:00:00",  # Same day, UK
            "2026-03-15 15:30:00",  # Same day, UK
            "2026-03-15 11:00:00",  # Same day, Germany
            "2026-03-16 10:00:00",  # Next day, UK
        ],
        "Country": ["United Kingdom", "United Kingdom", "Germany", "United Kingdom"],
        "CustomerID": ["111", "222", "333", "444"],
    })

    # Step 1: Check TotalPrice arithmetic
    price_df = calculate_total_price(clean_data)
    assert "TotalPrice" in price_df.columns
    # 10 * 2.50 = 25.0, 5 * 4.00 = 20.0, 20 * 1.50 = 30.0, 2 * 10.00 = 20.0
    expected_total_prices = [25.00, 20.00, 30.00, 20.00]
    np.testing.assert_allclose(price_df["TotalPrice"], expected_total_prices)

    # Step 2: Check aggregation by day-level InvoiceDate and Country
    agg_df = aggregate_daily_country_sales(price_df)

    assert "DailyTotalSales" in agg_df.columns
    assert "InvoiceDate" in agg_df.columns
    assert "Country" in agg_df.columns
    assert "Year" in agg_df.columns
    assert "Month" in agg_df.columns

    # Expected aggregations:
    # 2026-03-15 + UK: 25.00 + 20.00 = 45.00
    # 2026-03-15 + Germany: 30.00
    # 2026-03-16 + UK: 20.00
    assert len(agg_df) == 3

    uk_row_day1 = agg_df[(agg_df["InvoiceDate"] == "2026-03-15") & (agg_df["Country"] == "United Kingdom")]
    assert len(uk_row_day1) == 1
    assert uk_row_day1["DailyTotalSales"].iloc[0] == 45.00
    assert uk_row_day1["Year"].iloc[0] == 2026
    assert uk_row_day1["Month"].iloc[0] == 3

    ger_row = agg_df[(agg_df["InvoiceDate"] == "2026-03-15") & (agg_df["Country"] == "Germany")]
    assert len(ger_row) == 1
    assert ger_row["DailyTotalSales"].iloc[0] == 30.00

    uk_row_day2 = agg_df[(agg_df["InvoiceDate"] == "2026-03-16") & (agg_df["Country"] == "United Kingdom")]
    assert len(uk_row_day2) == 1
    assert uk_row_day2["DailyTotalSales"].iloc[0] == 20.00


def test_end_to_end_silver_transformation(tmp_path, sample_dirty_data):
    """
    Test Case 4: Verify end-to-end parquet-to-parquet transformation pipeline.
    """
    bronze_dir = str(tmp_path / "bronze")
    silver_dir = str(tmp_path / "silver")

    # Add Year and Month partitions to sample dirty data
    dirty = sample_dirty_data.copy()
    dirty["InvoiceDate"] = pd.to_datetime(dirty["InvoiceDate"])
    dirty["Year"] = dirty["InvoiceDate"].dt.year
    dirty["Month"] = dirty["InvoiceDate"].dt.month

    # Write bronze dataset
    dirty.to_parquet(bronze_dir, partition_cols=["Year", "Month"], index=False, engine="pyarrow")

    # Execute full transformation
    transform_to_silver(bronze_dir, silver_dir)

    # Read output
    result_df = pd.read_parquet(silver_dir, engine="pyarrow")
    assert len(result_df) > 0
    assert "DailyTotalSales" in result_df.columns
    assert "InvoiceDate" in result_df.columns
    assert "Country" in result_df.columns
    assert (result_df["DailyTotalSales"] >= 0).all()
