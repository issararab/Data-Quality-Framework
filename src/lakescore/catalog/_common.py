"""Shared DDL helpers used by every `catalog/*.py` table-store module.

Every statement here is fully catalog-qualified rather than relying on a `USE CATALOG`
"current catalog" session switch: `USE CATALOG` is a Databricks SQL extension that isn't part
of open-source Apache Spark's SQL grammar, so relying on it broke these helpers under the local
Spark session `tests/integration/` uses (`ParseException: extra input '<catalog>'`). Fully
qualifying every statement works on both Unity Catalog and plain Spark, and avoids depending on
mutable session state besides.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_schema_exists(
    spark: SparkSession, catalog_name: str, schema_name: str = "data_quality"
) -> None:
    """Creates `catalog_name.schema_name` if it doesn't already exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")


def table_exists(spark: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> bool:
    """Checks whether `table_name` exists in `catalog_name.schema_name`."""
    existing_tables = [
        row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
    ]
    return table_name in existing_tables
