"""Shared DDL helpers used by every `catalog/*.py` table-store module."""

from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_schema_exists(
    spark: SparkSession, catalog_name: str, schema_name: str = "data_quality"
) -> None:
    """Creates `catalog_name.schema_name` if it doesn't already exist, and sets it as the
    current catalog for subsequent unqualified DDL in this call."""
    spark.sql(f"USE CATALOG {catalog_name}")
    existing_schemas = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
    if schema_name not in existing_schemas:
        spark.sql(f"CREATE SCHEMA `{schema_name}`")


def table_exists(spark: SparkSession, schema_name: str, table_name: str) -> bool:
    """Checks whether `table_name` exists in `schema_name` of the current catalog."""
    existing_tables = [
        row.tableName for row in spark.sql(f"SHOW TABLES IN {schema_name}").collect()
    ]
    return table_name in existing_tables
