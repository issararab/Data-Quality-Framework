"""DDL-ensure and upsert for the `data_quality.dq_summary` table: the per-table LakeScore
scores surfaced on the dashboard."""

from __future__ import annotations

from pyspark.sql import SparkSession

from lakescore.catalog._common import ensure_schema_exists, table_exists

_BOOLEAN_COLUMNS = [
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


def get_dq_summary_target_table(spark: SparkSession, catalog_name: str) -> str:
    """Ensures `catalog_name.data_quality.dq_summary` exists, creating it (with all boolean
    metric columns defaulting to `False`) if needed.

    Returns:
        str: The fully qualified table path.
    """
    ensure_schema_exists(spark, catalog_name)
    if not table_exists(spark, "data_quality", "dq_summary"):
        spark.sql(
            f"""
            CREATE TABLE {catalog_name}.data_quality.dq_summary (
                table_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                catalog_name STRING,
                schema_name STRING,
                table_name STRING,
                table_description BOOLEAN,
                columns_description BOOLEAN,
                has_a_valid_owner BOOLEAN,
                is_delta_table BOOLEAN,
                uses_a_production_pipeline BOOLEAN,
                has_enforced_retention_duration BOOLEAN,
                is_managed_location BOOLEAN,
                is_fresh BOOLEAN,
                columns_valid BOOLEAN,
                columns_datatype_valid BOOLEAN,
                all_columns_have_checks BOOLEAN,
                table_implement_checks BOOLEAN,
                has_check_passed BOOLEAN
            )
            USING DELTA
            """
        )
        spark.sql(
            f"ALTER TABLE {catalog_name}.data_quality.dq_summary "
            "SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
        )
        for column in _BOOLEAN_COLUMNS:
            spark.sql(
                f"ALTER TABLE {catalog_name}.data_quality.dq_summary "
                f"ALTER COLUMN {column} SET DEFAULT False"
            )
    return f"{catalog_name}.data_quality.dq_summary"


def upsert_table_dq_summary(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    metrics: dict[str, bool],
) -> None:
    """Upserts the given `metrics` into the `dq_summary` row for one table, leaving all other
    columns untouched.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog the scored table (and `dq_summary`) live in.
        schema_name (str): Schema of the scored table.
        table_name (str): The scored table's name.
        metrics (dict[str, bool]): Metric column name -> value. Keys must exactly match
            `dq_summary` column names (no leading/trailing whitespace).
    """
    dq_summary_table_path = get_dq_summary_target_table(spark, catalog_name)

    update_columns = ", ".join(f"{key} = {value}" for key, value in metrics.items())
    insert_columns = ", ".join(metrics.keys())
    insert_values = ", ".join(str(value) for value in metrics.values())

    merge_sql = f"""
    MERGE INTO {dq_summary_table_path} AS target
    USING (SELECT '{catalog_name}' AS catalog_name,
                  '{schema_name}' AS schema_name,
                  '{table_name}' AS table_name) AS source
    ON target.catalog_name = source.catalog_name AND target.schema_name = source.schema_name AND target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET
        {update_columns}
    WHEN NOT MATCHED THEN INSERT (
        catalog_name, schema_name, table_name, {insert_columns}
    ) VALUES (
        '{catalog_name}', '{schema_name}', '{table_name}', {insert_values}
    );
    """

    spark.sql(merge_sql)
