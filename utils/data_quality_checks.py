"""Data Quality Checks Module

Utility functions for performing data quality validations across the
Hospital Management data pipeline.

Author: Data Engineering Team
Version: 1.0.0
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, isnan
from typing import List, Dict, Tuple


class DataQualityChecker:
    """Comprehensive data quality validation framework."""

    def __init__(self, df: DataFrame, table_name: str = "Unknown"):
        """
        Initialize DataQualityChecker.

        Args:
            df: Spark DataFrame to validate
            table_name: Name of the table for logging
        """
        self.df = df
        self.table_name = table_name
        self.issues = []

    def check_null_values(self, columns: List[str] = None) -> Dict[str, int]:
        """
        Check for null values in specified columns.

        Args:
            columns: List of column names to check. If None, checks all columns.

        Returns:
            Dictionary with column names and null counts
        """
        if columns is None:
            columns = self.df.columns

        null_counts = {}
        for column in columns:
            null_count = self.df.filter(col(column).isNull()).count()
            null_counts[column] = null_count

            if null_count > 0:
                self.issues.append(
                    f"Column '{column}' has {null_count} null values"
                )

        return null_counts

    def check_duplicates(self, columns: List[str] = None) -> int:
        """
        Check for duplicate records.

        Args:
            columns: List of columns to consider for duplicates.
                    If None, checks entire row.

        Returns:
            Count of duplicate records
        """
        if columns:
            duplicate_count = (
                self.df.groupBy(columns)
                .count()
                .filter("count > 1")
                .count()
            )
        else:
            total_rows = self.df.count()
            distinct_rows = self.df.distinct().count()
            duplicate_count = total_rows - distinct_rows

        if duplicate_count > 0:
            self.issues.append(f"Found {duplicate_count} duplicate records")

        return duplicate_count

    def check_empty_strings(self, columns: List[str]) -> Dict[str, int]:
        """
        Check for empty string values in specified columns.

        Args:
            columns: List of string column names to check

        Returns:
            Dictionary with column names and empty string counts
        """
        empty_counts = {}
        for column in columns:
            empty_count = self.df.filter(col(column) == "").count()
            empty_counts[column] = empty_count

            if empty_count > 0:
                self.issues.append(
                    f"Column '{column}' has {empty_count} empty strings"
                )

        return empty_counts

    def check_range(self, column: str, min_val: float, max_val: float) -> int:
        """
        Check if numeric values fall within expected range.

        Args:
            column: Column name to check
            min_val: Minimum acceptable value
            max_val: Maximum acceptable value

        Returns:
            Count of out-of-range values
        """
        out_of_range = self.df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()

        if out_of_range > 0:
            self.issues.append(
                f"Column '{column}' has {out_of_range} values "
                f"outside range [{min_val}, {max_val}]"
            )

        return out_of_range

    def check_schema(self, expected_columns: List[str]) -> List[str]:
        """
        Verify that DataFrame has expected columns.

        Args:
            expected_columns: List of expected column names

        Returns:
            List of missing columns
        """
        actual_columns = set(self.df.columns)
        expected_columns_set = set(expected_columns)
        missing_columns = list(expected_columns_set - actual_columns)

        if missing_columns:
            self.issues.append(
                f"Missing columns: {', '.join(missing_columns)}"
            )

        return missing_columns

    def check_record_count(self, min_records: int = 1) -> bool:
        """
        Check if DataFrame has minimum number of records.

        Args:
            min_records: Minimum expected record count

        Returns:
            True if record count is acceptable, False otherwise
        """
        record_count = self.df.count()

        if record_count < min_records:
            self.issues.append(
                f"Record count {record_count} is below minimum {min_records}"
            )
            return False

        return True

    def run_all_checks(
        self,
        null_check_columns: List[str] = None,
        duplicate_check_columns: List[str] = None,
        range_checks: Dict[str, Tuple[float, float]] = None,
        min_records: int = 1,
    ) -> Dict:
        """
        Run comprehensive data quality checks.

        Args:
            null_check_columns: Columns to check for nulls
            duplicate_check_columns: Columns to check for duplicates
            range_checks: Dict of {column: (min, max)} for range validation
            min_records: Minimum expected record count

        Returns:
            Dictionary with all check results
        """
        results = {
            "table_name": self.table_name,
            "total_records": self.df.count(),
            "total_columns": len(self.df.columns),
            "null_checks": {},
            "duplicate_count": 0,
            "range_checks": {},
            "issues": [],
        }

        # Check record count
        results["has_minimum_records"] = self.check_record_count(min_records)

        # Check nulls
        if null_check_columns:
            results["null_checks"] = self.check_null_values(null_check_columns)

        # Check duplicates
        if duplicate_check_columns:
            results["duplicate_count"] = self.check_duplicates(
                duplicate_check_columns
            )

        # Check ranges
        if range_checks:
            for column, (min_val, max_val) in range_checks.items():
                out_of_range = self.check_range(column, min_val, max_val)
                results["range_checks"][column] = out_of_range

        results["issues"] = self.issues
        results["has_issues"] = len(self.issues) > 0

        return results

    def print_summary(self):
        """Print data quality check summary."""
        print("=" * 60)
        print(f"Data Quality Report: {self.table_name}")
        print("=" * 60)
        print(f"Total Records: {self.df.count():,}")
        print(f"Total Columns: {len(self.df.columns)}")
        print()

        if self.issues:
            print("\u26a0️ Issues Found:")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")
        else:
            print("✅ No issues found! Data quality checks passed.")

        print("=" * 60)


def run_bronze_quality_checks(df: DataFrame, table_name: str) -> Dict:
    """
    Run quality checks for Bronze layer data.

    Args:
        df: Bronze layer DataFrame
        table_name: Name of the bronze table

    Returns:
        Quality check results dictionary
    """
    checker = DataQualityChecker(df, table_name)
    results = checker.run_all_checks(
        null_check_columns=["_id", "data"],
        duplicate_check_columns=["_id"],
        min_records=1,
    )
    checker.print_summary()
    return results


def run_silver_quality_checks(df: DataFrame, table_name: str) -> Dict:
    """
    Run quality checks for Silver layer data.

    Args:
        df: Silver layer DataFrame
        table_name: Name of the silver table

    Returns:
        Quality check results dictionary
    """
    checker = DataQualityChecker(df, table_name)
    results = checker.run_all_checks(
        null_check_columns=[
            "patient_id",
            "patient_name",
            "age",
            "gender",
            "city",
            "disease",
        ],
        duplicate_check_columns=["patient_id"],
        range_checks={"age": (0, 120), "bill_amount": (0, 1000000)},
        min_records=1,
    )
    checker.print_summary()
    return results


def run_gold_quality_checks(df: DataFrame, table_name: str) -> Dict:
    """
    Run quality checks for Gold layer data.

    Args:
        df: Gold layer DataFrame
        table_name: Name of the gold table

    Returns:
        Quality check results dictionary
    """
    checker = DataQualityChecker(df, table_name)
    results = checker.run_all_checks(min_records=1)
    checker.print_summary()
    return results