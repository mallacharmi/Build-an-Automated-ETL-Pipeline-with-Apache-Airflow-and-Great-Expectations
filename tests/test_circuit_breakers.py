"""
Unit and integration tests for Great Expectations circuit breaker validation.
Verifies that:
- Clean Bronze data passes bronze validation
- Invalid Bronze data raises RuntimeError (halting the pipeline)
- Clean Silver data passes silver validation
- Invalid Silver data raises RuntimeError (halting the pipeline)
"""

import os
import pandas as pd
import pytest

from etl_scripts.validator import (
    validate_dataframe_against_suite,
    validate_bronze_layer,
    validate_silver_layer,
)

BRONZE_SUITE_PATH = "great_expectations/expectations/bronze_expectations.json"
SILVER_SUITE_PATH = "great_expectations/expectations/silver_expectations.json"


@pytest.fixture
def valid_bronze_df():
    """Valid bronze dataset matching bronze_expectations.json schema."""
    return pd.DataFrame({
        "InvoiceNo": ["536365", "536366", "536367"],
        "StockCode": ["85123A", "71053", "84406B"],
        "Quantity": [6, 2, 8],
        "InvoiceDate": ["2026-01-10 08:26:00", "2026-01-10 09:15:00", "2026-01-10 14:30:00"],
        "UnitPrice": [2.55, 3.39, 2.75],
        "CustomerID": ["17850", "13047", "17850"],
        "Country": ["United Kingdom", "United Kingdom", "United Kingdom"],
        "Year": [2026, 2026, 2026],
        "Month": [1, 1, 1],
    })


@pytest.fixture
def valid_silver_df():
    """Valid silver dataset matching silver_expectations.json schema."""
    return pd.DataFrame({
        "InvoiceDate": ["2026-01-10", "2026-01-11", "2026-01-12"],
        "Country": ["United Kingdom", "United Kingdom", "Germany"],
        "DailyTotalSales": [150.50, 220.00, 75.25],
        "Year": [2026, 2026, 2026],
        "Month": [1, 1, 1],
    })


def test_valid_bronze_validation(valid_bronze_df):
    """Assert valid bronze data passes all schema expectations."""
    passed, failures = validate_dataframe_against_suite(
        valid_bronze_df, BRONZE_SUITE_PATH, "bronze_expectations"
    )
    assert passed is True
    assert len(failures) == 0


def test_bronze_circuit_breaker_on_missing_column(valid_bronze_df):
    """Assert bronze validator trips circuit breaker when required column is missing."""
    corrupted_df = valid_bronze_df.drop(columns=["InvoiceNo"])
    passed, failures = validate_dataframe_against_suite(
        corrupted_df, BRONZE_SUITE_PATH, "bronze_expectations"
    )
    assert passed is False
    assert any("InvoiceNo" in f for f in failures)


def test_bronze_circuit_breaker_on_invalid_type(valid_bronze_df):
    """Assert bronze validator trips when column has wrong data type (e.g. string for Quantity)."""
    corrupted_df = valid_bronze_df.copy()
    corrupted_df["Quantity"] = ["invalid_qty", "text", "bad"]
    passed, failures = validate_dataframe_against_suite(
        corrupted_df, BRONZE_SUITE_PATH, "bronze_expectations"
    )
    assert passed is False
    assert any("Quantity" in f for f in failures)


def test_valid_silver_validation(valid_silver_df):
    """Assert valid silver data passes all business rule expectations."""
    passed, failures = validate_dataframe_against_suite(
        valid_silver_df, SILVER_SUITE_PATH, "silver_expectations"
    )
    assert passed is True
    assert len(failures) == 0


def test_silver_circuit_breaker_on_negative_sales(valid_silver_df):
    """Assert silver validator trips when DailyTotalSales has negative values."""
    corrupted_df = valid_silver_df.copy()
    corrupted_df.loc[0, "DailyTotalSales"] = -50.0
    passed, failures = validate_dataframe_against_suite(
        corrupted_df, SILVER_SUITE_PATH, "silver_expectations"
    )
    assert passed is False
    assert any("DailyTotalSales" in f for f in failures)


def test_silver_circuit_breaker_on_duplicate_composite_keys(valid_silver_df):
    """Assert silver validator trips when composite key (InvoiceDate, Country) contains duplicates."""
    duplicate_row = valid_silver_df.iloc[[0]].copy()
    corrupted_df = pd.concat([valid_silver_df, duplicate_row], ignore_index=True)
    passed, failures = validate_dataframe_against_suite(
        corrupted_df, SILVER_SUITE_PATH, "silver_expectations"
    )
    assert passed is False
    assert any("duplicate" in f.lower() or "unique" in f.lower() for f in failures)


def test_validate_bronze_layer_raises_exception_on_corrupted_directory(tmp_path, valid_bronze_df):
    """Verify validate_bronze_layer raises RuntimeError when evaluating invalid data."""
    bad_dir = str(tmp_path / "bad_bronze")
    bad_df = valid_bronze_df.drop(columns=["StockCode"])
    bad_df.to_parquet(bad_dir, index=False)

    with pytest.raises(RuntimeError) as exc_info:
        validate_bronze_layer(bad_dir, BRONZE_SUITE_PATH)

    assert "FAILED" in str(exc_info.value)
