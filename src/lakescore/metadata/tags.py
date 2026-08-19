"""Unity Catalog column tag mutations, plus reconciliation of the `has_check` tag against the
actual contents of `column_checks`.

`set_column_tag`/`unset_column_tag` are the single shared primitives for all of LakeScore's
tag-based state (`has_check`, `has_comment`, `has_low_cardinality`) — callers should not build
`ALTER TABLE ... TAGS` statements themselves.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakescore.catalog.column_checks import (
    get_checks_target_table,
    get_columns_with_checks,
    sync_checks_target_table,
)
from lakescore.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


def set_column_tag(
    spark: SparkSession, table: str, column: str, tag_name: str, tag_value: str | None = None
) -> bool:
    """Sets `tag_name` (optionally with a value) on `table.column`.

    Parameters:
        spark (SparkSession): Active Spark session.
        table (str): Fully qualified table name.
        column (str): Column to tag.
        tag_name (str): Tag name (e.g. `"has_check"`, `"has_low_cardinality"`).
        tag_value (str | None): Optional tag value; escaped before being spliced into the
            statement since it may originate from raw column data.

    Returns:
        bool: `True` on success, `False` if the `ALTER TABLE` failed.
    """
    value_clause = f" = '{escape_sql_string(tag_value)}'" if tag_value is not None else ""
    try:
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag_name}'{value_clause});"
        )
        return True
    except Exception as e:
        logger.error("Error while setting tag '%s' on %s.%s: %s", tag_name, table, column, e)
        return False


def unset_column_tag(spark: SparkSession, table: str, column: str, tag_name: str) -> bool:
    """Removes `tag_name` from `table.column`.

    Returns:
        bool: `True` on success, `False` if the `ALTER TABLE` failed.
    """
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} UNSET TAGS ('{tag_name}');")
        return True
    except Exception as e:
        logger.error("Error while unsetting tag '%s' on %s.%s: %s", tag_name, table, column, e)
        return False


def _get_columns_with_invalid_check_tags(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> dict[str, list[str]]:
    """Finds columns tagged `has_check` in Unity Catalog that no longer have a matching row in
    `column_checks` (e.g. the check was deleted without clearing the tag)."""
    checks_table_path = get_checks_target_table(spark, catalog_name)
    checks_df = spark.read.table(checks_table_path)

    filtered_checks_df = (
        checks_df.filter(
            (F.col("catalog_name") == catalog_name)
            & (F.col("schema_name") == schema_name)
            & (F.col("table_name").isin(list_of_tables))
            & (F.col("check").isNotNull())
        )
        .select("catalog_name", "schema_name", "table_name", "column_name", "check")
        .distinct()
    )

    tags_df = spark.read.table(f"{catalog_name}.information_schema.column_tags")
    filtered_tags_df = tags_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("tag_name") == "has_check")
        & (F.col("table_name").isin(list_of_tables))
    ).select("catalog_name", "schema_name", "table_name", "column_name")

    joined_df = (
        filtered_tags_df.join(
            filtered_checks_df,
            on=[
                filtered_checks_df.catalog_name == filtered_tags_df.catalog_name,
                filtered_checks_df.schema_name == filtered_tags_df.schema_name,
                filtered_checks_df.table_name == filtered_tags_df.table_name,
                filtered_checks_df.column_name == filtered_tags_df.column_name,
            ],
            how="left",
        )
        .filter(F.col("check").isNull())
        .select(
            filtered_tags_df["catalog_name"],
            filtered_tags_df["schema_name"],
            filtered_tags_df["table_name"],
            filtered_tags_df["column_name"],
        )
    )

    grouped_df = joined_df.groupBy("catalog_name", "schema_name", "table_name").agg(
        F.collect_list("column_name").alias("columns")
    )
    return grouped_df.rdd.map(
        lambda row: (
            f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}",
            row["columns"],
        )
    ).collectAsMap()


def update_checks_tag(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> str:
    """Reconciles the `has_check` column tag with `column_checks`: syncs out stale check rows,
    unsets `has_check` where it no longer applies, and sets it where a valid check exists.

    Imports `lakescore.catalog.column_checks` for the sync step and the "which columns have a
    valid check" read, keeping the `column_checks` table's own mutation logic in one place.
    """
    sync_checks_target_table(spark, catalog_name, schema_name, list_of_tables)

    for table, columns in _get_columns_with_invalid_check_tags(
        spark, catalog_name, schema_name, list_of_tables
    ).items():
        for column in columns:
            unset_column_tag(spark, table, column, "has_check")

    for table, columns in get_columns_with_checks(
        spark, catalog_name, schema_name, list_of_tables
    ).items():
        for column in columns:
            set_column_tag(spark, table, column, "has_check")

    return "Check tags updated!"
