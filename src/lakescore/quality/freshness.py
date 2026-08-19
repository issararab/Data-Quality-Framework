"""Freshness dimension: is a table's most recent write within its configured window."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from lakescore.metadata.tables import full_table_name

_THRESHOLD_PATTERN = re.compile(r"(\d+)([dhm])")

_WRITE_OPERATIONS = (
    "WRITE",
    "CREATE TABLE",
    "CREATE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE",
    "CREATE OR REPLACE TABLE AS SELECT",
    "COPY INTO",
    "STREAMING UPDATE",
    "MERGE",
    "UPDATE",
    "DELETE",
)


def parse_threshold(threshold_str: str) -> timedelta:
    """Parses a threshold string like `"3d"`, `"1h"`, `"30m"`, or `"1d6h"` into a `timedelta`.

    Raises:
        ValueError: If `threshold_str` contains no recognizable `<number><d|h|m>` component.
    """
    matches = _THRESHOLD_PATTERN.findall(threshold_str)
    if not matches:
        raise ValueError(f"Invalid threshold format: {threshold_str}")

    days = hours = minutes = 0
    for value, unit in matches:
        if unit == "d":
            days += int(value)
        elif unit == "h":
            hours += int(value)
        elif unit == "m":
            minutes += int(value)
    return timedelta(days=days, hours=hours, minutes=minutes)


def extract_time_components(delta: timedelta) -> tuple[int, int, int]:
    """Splits a `timedelta` into `(days, hours, minutes)`."""
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes = remainder // 60
    return days, hours, minutes


def check_table_freshness(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    list_of_tables: list[str],
    freshness_threshold: str,
) -> list[dict[str, Any]]:
    """Checks whether each table's most recent qualifying write is within `freshness_threshold`.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog the tables live in.
        schema_name (str): Schema the tables live in.
        list_of_tables (list[str]): Table names to check.
        freshness_threshold (str): Window string, e.g. `"1d"`.

    Returns:
        list[dict[str, Any]]: One dict per table with `table_name`, `freshness_threshold`,
            `last_update`, `is_data_fresh`, `time_since_last_update`.
    """
    interval = parse_threshold(freshness_threshold)
    results = []

    for table_name in (full_table_name(catalog_name, schema_name, t) for t in list_of_tables):
        operations = ", ".join(f"'{op}'" for op in _WRITE_OPERATIONS)
        last_mod_time = spark.sql(
            f"""
            WITH history_table AS (DESCRIBE HISTORY {table_name})
            SELECT max(timestamp) as last_update
            FROM history_table
            WHERE operation IN ({operations})
            """
        ).collect()[0]["last_update"]

        date_diff = datetime.utcnow() - last_mod_time
        days, hours, minutes = extract_time_components(date_diff)

        results.append(
            {
                "table_name": table_name,
                "freshness_threshold": freshness_threshold,
                "last_update": str(last_mod_time),
                "is_data_fresh": date_diff < interval,
                "time_since_last_update": f"{days}d {hours}h {minutes}m",
            }
        )

    return results
