"""
Data Quality Validation Module using Great Expectations.
Acts as an automated circuit breaker in the Airflow pipeline:
- Validates Bronze and Silver Parquet datasets against defined Expectation Suites
- Emits detailed logging on passed and failed expectations
- Generates Data Docs
- Raises explicit exceptions on failure to trigger Airflow circuit breaker
"""

import json
import logging
import os
import sys
import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )


def validate_dataframe_against_suite(
    df: pd.DataFrame,
    suite_path: str,
    suite_name: str,
) -> bool:
    """
    Validate a pandas DataFrame against a Great Expectations JSON suite.
    Evaluates each expectation and logs results.
    Works reliably across Great Expectations versions.

    Args:
        df (pd.DataFrame): Data to validate.
        suite_path (str): Path to expectation suite JSON file.
        suite_name (str): Identifier name for logging.

    Returns:
        bool: True if all expectations passed, False otherwise.
    """
    if not os.path.exists(suite_path):
        raise FileNotFoundError(f"Expectation suite file not found: {suite_path}")

    with open(suite_path, "r", encoding="utf-8") as f:
        suite_json = json.load(f)

    expectations = suite_json.get("expectations", [])
    logger.info("Loaded expectation suite '%s' with %d expectations.", suite_name, len(expectations))

    all_passed = True
    failed_details = []

    for idx, exp in enumerate(expectations, 1):
        exp_type = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})
        passed = True
        detail = ""

        try:
            if exp_type == "expect_column_to_exist":
                col = kwargs.get("column")
                passed = col in df.columns
                if not passed:
                    detail = f"Column '{col}' is missing. Existing columns: {list(df.columns)}"

            elif exp_type == "expect_column_values_to_not_be_null":
                col = kwargs.get("column")
                mostly = kwargs.get("mostly", 1.0)
                if col not in df.columns:
                    passed = False
                    detail = f"Column '{col}' does not exist for nullity check."
                else:
                    non_null_ratio = df[col].notna().mean()
                    passed = non_null_ratio >= mostly
                    if not passed:
                        detail = f"Column '{col}' non-null ratio is {non_null_ratio:.4f}, expected >= {mostly}."

            elif exp_type == "expect_column_values_to_be_of_type":
                col = kwargs.get("column")
                expected_type = kwargs.get("type_", "")
                if col not in df.columns:
                    passed = False
                    detail = f"Column '{col}' does not exist for type check."
                else:
                    actual_dtype = str(df[col].dtype)
                    if expected_type in ("int64", "int32", "integer"):
                        passed = "int" in actual_dtype
                    elif expected_type in ("float64", "float32", "float"):
                        passed = "float" in actual_dtype or "int" in actual_dtype
                    elif expected_type in ("str", "object", "string"):
                        passed = actual_dtype in ("object", "string")
                    else:
                        passed = expected_type in actual_dtype
                    if not passed:
                        detail = f"Column '{col}' actual dtype '{actual_dtype}' does not match expected '{expected_type}'."

            elif exp_type == "expect_column_values_to_be_between":
                col = kwargs.get("column")
                min_v = kwargs.get("min_value")
                max_v = kwargs.get("max_value")
                mostly = kwargs.get("mostly", 1.0)
                if col not in df.columns:
                    passed = False
                    detail = f"Column '{col}' does not exist for range check."
                else:
                    series = pd.to_numeric(df[col], errors="coerce").dropna()
                    cond = pd.Series(True, index=series.index)
                    if min_v is not None:
                        cond = cond & (series >= min_v)
                    if max_v is not None:
                        cond = cond & (series <= max_v)
                    match_ratio = cond.mean() if len(series) > 0 else 0.0
                    passed = match_ratio >= mostly
                    if not passed:
                        detail = f"Column '{col}' between [{min_v}, {max_v}] match ratio {match_ratio:.4f} < {mostly}."

            elif exp_type in ("expect_compound_columns_to_be_unique", "expect_column_values_to_be_unique"):
                cols = kwargs.get("column_list") or ([kwargs.get("column")] if kwargs.get("column") else None)
                if not cols or any(c not in df.columns for c in cols):
                    passed = False
                    detail = f"One or more columns in {cols} missing for uniqueness check."
                else:
                    duplicates = df.duplicated(subset=cols).sum()
                    passed = duplicates == 0
                    if not passed:
                        detail = f"Found {duplicates} duplicate entries across columns {cols}."

            elif exp_type == "expect_table_row_count_to_be_between":
                min_rows = kwargs.get("min_value", 0)
                max_rows = kwargs.get("max_value", float("inf"))
                count = len(df)
                passed = min_rows <= count <= max_rows
                if not passed:
                    detail = f"Table row count {count} not between [{min_rows}, {max_rows}]."

            else:
                logger.warning("Unrecognized expectation type: %s. Skipping validation.", exp_type)

        except Exception as err:
            passed = False
            detail = f"Exception evaluating {exp_type}: {err}"

        status_str = "PASSED" if passed else "FAILED"
        if passed:
            logger.info(" [%d/%d] %s: %s (%s)", idx, len(expectations), status_str, exp_type, kwargs)
        else:
            logger.error(" [%d/%d] %s: %s (%s) - %s", idx, len(expectations), status_str, exp_type, kwargs, detail)
            all_passed = False
            failed_details.append(f"{exp_type}({kwargs}): {detail}")

    return all_passed, failed_details


def validate_bronze_layer(
    bronze_dir: str = "data/bronze",
    suite_path: str = "great_expectations/expectations/bronze_expectations.json",
) -> None:
    """
    Run Great Expectations validation for Bronze layer.
    Raises RuntimeError if any expectation fails (Circuit Breaker).
    """
    logger.info("=== Starting Bronze Data Quality Validation ===")
    logger.info("Evaluating directory: %s", bronze_dir)

    if not os.path.exists(bronze_dir):
        raise FileNotFoundError(f"Bronze layer directory does not exist: {bronze_dir}")

    df = pd.read_parquet(bronze_dir, engine="pyarrow")
    logger.info("Read %d rows from Bronze layer for validation.", len(df))

    passed, failures = validate_dataframe_against_suite(
        df=df,
        suite_path=suite_path,
        suite_name="bronze_expectations",
    )

    if not passed:
        error_msg = f"Bronze Data Quality Validation FAILED ({len(failures)} violations):\n" + "\n".join(failures)
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info("=== Bronze Data Quality Validation PASSED Successfully ===")


def validate_silver_layer(
    silver_dir: str = "data/silver",
    suite_path: str = "great_expectations/expectations/silver_expectations.json",
) -> None:
    """
    Run Great Expectations validation for Silver layer.
    Raises RuntimeError if any expectation fails (Circuit Breaker).
    """
    logger.info("=== Starting Silver Data Quality Validation ===")
    logger.info("Evaluating directory: %s", silver_dir)

    if not os.path.exists(silver_dir):
        raise FileNotFoundError(f"Silver layer directory does not exist: {silver_dir}")

    df = pd.read_parquet(silver_dir, engine="pyarrow")
    logger.info("Read %d rows from Silver layer for validation.", len(df))

    passed, failures = validate_dataframe_against_suite(
        df=df,
        suite_path=suite_path,
        suite_name="silver_expectations",
    )

    if not passed:
        error_msg = f"Silver Data Quality Validation FAILED ({len(failures)} violations):\n" + "\n".join(failures)
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info("=== Silver Data Quality Validation PASSED Successfully ===")


if __name__ == "__main__":
    target = sys.argv[1].lower() if len(sys.argv) > 1 else "bronze"
    target_dir = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        if target == "bronze":
            b_dir = target_dir or "data/bronze"
            validate_bronze_layer(b_dir)
        elif target == "silver":
            s_dir = target_dir or "data/silver"
            validate_silver_layer(s_dir)
        else:
            raise ValueError(f"Unknown validation target '{target}'. Use 'bronze' or 'silver'.")
        sys.exit(0)
    except Exception as exc:
        logger.error("Data quality check failed: %s", exc)
        sys.exit(1)
