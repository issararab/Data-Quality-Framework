"""DDL-ensure and CRUD for the `data_quality.column_checks` table, plus reconciliation against
the live warehouse (dropping checks left behind by dropped columns/tables)."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lakescore.catalog._common import ensure_schema_exists, table_exists

logger = logging.getLogger(__name__)

_SCHEMA = StructType(
    [
        StructField("check_id", IntegerType(), True),
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("check", StringType(), False),
        StructField("tag", StringType(), True),
    ]
)


def get_checks_target_table(spark: SparkSession, catalog_name: str) -> str:
    """Ensures `catalog_name.data_quality.column_checks` exists, creating it (with a `NULL`
    default on `tag`) if needed.

    Returns:
        str: The fully qualified table path.
    """
    ensure_schema_exists(spark, catalog_name)
    if not table_exists(spark, catalog_name, "data_quality", "column_checks"):
        spark.sql(
            f"""
            CREATE TABLE {catalog_name}.data_quality.column_checks (
                check_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
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
            f"ALTER TABLE {catalog_name}.data_quality.column_checks "
            "SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
        )
        spark.sql(
            f"ALTER TABLE {catalog_name}.data_quality.column_checks ALTER COLUMN tag SET DEFAULT NULL"
        )
    return f"{catalog_name}.data_quality.column_checks"


def update_column_checks(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    check: str,
    tag: str | None = None,
    check_id: int | None = None,
) -> bool:
    """Upserts a check for one column.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog owning both the target table and `column_checks`.
        schema_name (str): Schema of the target table.
        table_name (str): The target table's name.
        column_name (str): The target column's name.
        check (str): The SodaCL check line(s).
        tag (str | None): `"ai_generated"` for LLM-authored checks, `None` otherwise.
        check_id (int | None): If given, targets this specific existing row instead of matching
            on `(catalog, schema, table, column, tag)`.

    Returns:
        bool: `True` on success, `False` if the write/merge failed.
    """
    target_table = get_checks_target_table(spark, catalog_name)

    new_df = spark.createDataFrame(
        [(check_id, catalog_name, schema_name, table_name, column_name, check, tag)], schema=_SCHEMA
    )
    new_df.createOrReplaceTempView("new_checks")

    if check_id is None:
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING new_checks AS source
        ON target.catalog_name = source.catalog_name
          AND target.schema_name = source.schema_name
          AND target.table_name = source.table_name
          AND target.column_name = source.column_name
          AND target.tag = source.tag
        WHEN MATCHED THEN
          UPDATE SET target.check = source.check
        WHEN NOT MATCHED THEN
          INSERT (catalog_name, schema_name, table_name, column_name, check, tag)
          VALUES (source.catalog_name, source.schema_name, source.table_name, source.column_name, source.check, source.tag)
        """
    else:
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING new_checks AS source
        ON target.check_id = source.check_id
          AND target.schema_name = source.schema_name
          AND target.table_name = source.table_name
          AND target.column_name = source.column_name
        WHEN MATCHED THEN
          UPDATE SET target.check = source.check,
                      target.tag = source.tag
        """

    try:
        if spark.table(target_table).count() == 0:
            new_df.drop("check_id").write.option("mergeSchema", "true").format("delta").mode(
                "append"
            ).saveAsTable(target_table)
        else:
            spark.sql(merge_sql)
        return True
    except Exception as e:
        logger.error("Error while writing to %s: %s", target_table, e)
        return False


def add_column_check(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    check: str,
) -> bool:
    """Inserts a new (manually authored) check for one column, without upsert matching.

    Returns:
        bool: `True` on success, `False` if the insert failed.
    """
    target_table = get_checks_target_table(spark, catalog_name)
    schema = StructType(
        [
            StructField("catalog_name", StringType(), False),
            StructField("schema_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("check", StringType(), False),
        ]
    )
    new_df = spark.createDataFrame(
        [(catalog_name, schema_name, table_name, column_name, check)], schema=schema
    )
    try:
        new_df.write.format("delta").mode("append").saveAsTable(target_table)
        return True
    except Exception as e:
        logger.error("Error while inserting into %s: %s", target_table, e)
        return False


def _drop_column_checks(
    spark: SparkSession, checks_table_path: str, check_ids_to_remove: list[int]
) -> bool:
    """Deletes rows from `column_checks` by `check_id`. No-op (returns `True`) if the list is
    empty."""
    if not check_ids_to_remove:
        return True
    try:
        ids = ", ".join(str(i) for i in check_ids_to_remove)
        spark.sql(f"DELETE FROM {checks_table_path} WHERE check_id IN ({ids})")
        return True
    except Exception as e:
        logger.error("Error while deleting from %s: %s", checks_table_path, e)
        return False


def sync_checks_target_table(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> bool:
    """Removes `column_checks` rows that are stale: empty `check` text, or referencing a
    column that no longer exists in the warehouse.

    Returns:
        bool: Result of the underlying delete, or `True` if nothing needed removing.
    """
    checks_table_path = get_checks_target_table(spark, catalog_name)
    checks_df = spark.read.table(checks_table_path)

    check_ids_to_remove = (
        checks_df.filter(
            (F.col("catalog_name") == catalog_name)
            & (F.col("schema_name") == schema_name)
            & (F.col("table_name").isin(list_of_tables))
            & (F.col("check").isNull())
        )
        .select("check_id")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    filtered_checks_df = (
        checks_df.filter(
            (F.col("catalog_name") == catalog_name)
            & (F.col("schema_name") == schema_name)
            & (F.col("table_name").isin(list_of_tables))
            & (F.col("check").isNotNull())
        )
        .select("check_id", "catalog_name", "schema_name", "table_name", "column_name")
        .distinct()
    )

    columns_df = spark.read.table(f"{catalog_name}.information_schema.columns")
    filtered_columns_df = (
        columns_df.filter(
            (F.col("table_catalog") == catalog_name)
            & (F.col("table_schema") == schema_name)
            & (F.col("table_name").isin(list_of_tables))
        )
        .select("table_catalog", "table_schema", "table_name", "column_name", "data_type")
        .distinct()
    )

    orphaned_ids = (
        filtered_checks_df.join(
            filtered_columns_df,
            on=[
                filtered_columns_df.table_catalog == filtered_checks_df.catalog_name,
                filtered_columns_df.table_schema == filtered_checks_df.schema_name,
                filtered_columns_df.table_name == filtered_checks_df.table_name,
                filtered_columns_df.column_name == filtered_checks_df.column_name,
            ],
            how="left",
        )
        .filter(F.col("data_type").isNull())
        .select("check_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    return _drop_column_checks(spark, checks_table_path, [*check_ids_to_remove, *orphaned_ids])


def get_columns_with_checks(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> dict[str, list[str]]:
    """Returns, per fully qualified table name, the columns that currently have a non-null
    check recorded in `column_checks`."""
    checks_table_path = get_checks_target_table(spark, catalog_name)
    tags_df = spark.read.table(checks_table_path)

    filtered_df = (
        tags_df.filter(
            (F.col("catalog_name") == catalog_name)
            & (F.col("schema_name") == schema_name)
            & (F.col("table_name").isin(list_of_tables))
            & (F.col("check").isNotNull())
        )
        .select("catalog_name", "schema_name", "table_name", "column_name")
        .distinct()
    )

    grouped_df = filtered_df.groupBy("catalog_name", "schema_name", "table_name").agg(
        F.collect_list("column_name").alias("columns")
    )
    return grouped_df.rdd.map(
        lambda row: (
            f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}",
            row["columns"],
        )
    ).collectAsMap()
