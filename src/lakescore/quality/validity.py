"""Validity dimension: has a table's schema drifted (columns/datatypes) since a past version."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from lakescore.metadata.tables import full_table_name
from lakescore.quality.freshness import parse_threshold


def _get_schema_versioned(spark: SparkSession, table_name: str, target_day: str) -> StructType:
    """Returns the Delta table's schema as of midnight of `target_day` (or the earliest
    available version, if `target_day` predates the table's history).

    **Note:** Delta retains history for 30 days by default (configurable). If `target_day`
    exceeds the retention period, the earliest available version's schema is returned instead.

    Parameters:
        spark (SparkSession): Active Spark session.
        table_name (str): Fully qualified table name.
        target_day (str): Target date, `"YYYY-MM-DD HH:MM:SS"`.
    """
    target_date = datetime.strptime(target_day, "%Y-%m-%d %H:%M:%S").replace(
        hour=0, minute=0, second=0
    )
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")

    min_timestamp = history_df.agg({"timestamp": "min"}).collect()[0][0]
    if target_date < min_timestamp:
        version = history_df.orderBy("timestamp").limit(1).select("version").collect()[0]["version"]
    else:
        version = (
            history_df.filter(history_df.timestamp <= target_date)
            .orderBy(history_df.timestamp.desc())
            .limit(1)
            .select("version")
            .collect()[0]["version"]
        )

    return spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {version} LIMIT 1").schema


def compare_table_schema_with_version(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    list_of_tables: list[str],
    threshold_str: str,
) -> list[dict[str, Any]]:
    """Compares each table's current schema against its schema as of `threshold_str` ago.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog the tables live in.
        schema_name (str): Schema the tables live in.
        list_of_tables (list[str]): Table names to compare.
        threshold_str (str): How far back to compare, e.g. `"3d"`.

    Returns:
        list[dict[str, Any]]: One dict per table with `table_name`, `threshold`, `columns_match`,
            `datatypes_match`, `mismatched_datatypes`, `col_missing_in_current_table`,
            `col_missing_in_old_table`.
    """
    threshold_timedelta = parse_threshold(threshold_str)
    results = []

    for table_name in (full_table_name(catalog_name, schema_name, t) for t in list_of_tables):
        current_types = {
            f.name: str(f.dataType) for f in spark.sql(f"SELECT * FROM {table_name}").schema.fields
        }

        window_date_str = (datetime.utcnow() - threshold_timedelta).strftime("%Y-%m-%d %H:%M:%S")
        versioned_types = {
            f.name: str(f.dataType)
            for f in _get_schema_versioned(spark, table_name, window_date_str).fields
        }

        current_columns = set(current_types)
        versioned_columns = set(versioned_types)
        common_columns = current_columns & versioned_columns
        columns_match = current_columns == versioned_columns

        mismatched_datatypes = {
            col: {"current": current_types[col], "versioned": versioned_types[col]}
            for col in common_columns
            if current_types[col] != versioned_types[col]
        }

        results.append(
            {
                "table_name": table_name,
                "threshold": threshold_str,
                "columns_match": columns_match,
                "datatypes_match": len(mismatched_datatypes) == 0,
                "mismatched_datatypes": mismatched_datatypes,
                "col_missing_in_current_table": None
                if columns_match
                else versioned_columns - current_columns,
                "col_missing_in_old_table": None
                if columns_match
                else current_columns - versioned_columns,
            }
        )

    return results
