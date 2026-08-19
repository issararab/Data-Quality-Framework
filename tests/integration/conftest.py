"""Fixtures scoped to `tests/integration/` only (not `tests/unit/`, which shouldn't ever try to
start Spark)."""

from __future__ import annotations

import pytest

_DQ_SUMMARY_BOOLEAN_COLUMNS = [
    "table_description",
    "columns_description",
    "has_a_valid_owner",
    "is_delta_table",
    "uses_a_production_pipeline",
    "has_enforced_retention_duration",
    "is_managed_location",
    "is_fresh",
    "columns_valid",
    "columns_datatype_valid",
    "all_columns_have_checks",
    "table_implement_checks",
    "has_check_passed",
]


@pytest.fixture(scope="session", autouse=True)
def _pre_create_governed_tables(spark):
    """Pre-creates `dq_config`/`column_checks`/`dq_summary` with production's exact schema,
    minus the `GENERATED ALWAYS AS IDENTITY` column.

    Databricks' `GENERATED ALWAYS AS IDENTITY (...)` column syntax isn't recognized by the SQL
    parser in open-source Spark + Delta (confirmed against both delta-spark 3.2.0 and 3.3.2) —
    it's a Databricks Runtime SQL extension, not something achievable with a config flag.
    `lakescore.catalog.*.get_*_target_table` functions only run their `CREATE TABLE` branch when
    the table doesn't already exist, so pre-creating it here — with the same columns everything
    else in this module operates on, just without the identity column nothing in the MERGE/
    upsert logic under test ever references — lets every test exercise the real CRUD/MERGE code
    path for real, without going through the one DDL branch that simply can't run outside
    Databricks. `ALTER COLUMN ... SET DEFAULT` (used for dq_summary's boolean defaults) *does*
    work on OSS Delta, so those are replicated faithfully here.
    """
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.data_quality")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS spark_catalog.data_quality.dq_config (
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            low_cardinality_threshold INT,
            freshness_window STRING,
            validity_window STRING,
            tag STRING
        )
        USING DELTA
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS spark_catalog.data_quality.column_checks (
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            column_name STRING,
            check STRING,
            tag STRING
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.data_quality.dq_summary (
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            {", ".join(f"{c} BOOLEAN" for c in _DQ_SUMMARY_BOOLEAN_COLUMNS)}
        )
        USING DELTA
        """
    )
    spark.sql(
        "ALTER TABLE spark_catalog.data_quality.dq_summary "
        "SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
    )
    for column in _DQ_SUMMARY_BOOLEAN_COLUMNS:
        spark.sql(
            f"ALTER TABLE spark_catalog.data_quality.dq_summary ALTER COLUMN {column} SET DEFAULT False"
        )
