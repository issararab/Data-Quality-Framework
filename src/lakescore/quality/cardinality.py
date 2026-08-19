"""Low-cardinality column detection and tagging, feeding the `2_metadata_update` step's decision
to skip check/comment generation for enum-like columns."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

from lakescore.metadata.columns import get_table_columns
from lakescore.metadata.tags import set_column_tag, unset_column_tag


def identify_low_cardinality_columns(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    list_of_tables: list[str],
    threshold: int = 10,
) -> list[dict[str, Any]]:
    """Classifies each column in `list_of_tables` as low- or high-cardinality by distinct value
    count (or by an existing `has_low_cardinality` tag, which short-circuits recomputation).

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog the tables live in.
        schema_name (str): Schema the tables live in.
        list_of_tables (list[str]): Table names to check.
        threshold (int): Max distinct values to classify a column as low-cardinality.

    Returns:
        list[dict[str, Any]]: One dict per column with `table_catalog`, `table_schema`,
            `table_name`, `column_name`, `is_low_cardinality`, `distinct_values` (comma-joined,
            or `None` when not low-cardinality).
    """
    column_info = json.loads(get_table_columns(spark, catalog_name, schema_name, list_of_tables))
    results = []

    for table in list_of_tables:
        table_df = spark.read.table(f"{catalog_name}.{schema_name}.{table}")
        table_columns = [
            (obj["column_name"], obj["tag_names"])
            for obj in column_info
            if obj["table_name"] == table
        ]

        for column, tag_names in table_columns:
            if "has_low_cardinality" in tag_names:
                is_low_cardinality = True
                distinct_values: list[Any] = (
                    table_df.select(column).agg(collect_set(column).alias("v")).collect()[0]["v"]
                )
            else:
                distinct_values = (
                    table_df.select(column).agg(collect_set(column).alias("v")).collect()[0]["v"]
                )
                is_low_cardinality = len(distinct_values) <= threshold

            results.append(
                {
                    "table_catalog": catalog_name,
                    "table_schema": schema_name,
                    "table_name": table,
                    "column_name": column,
                    "is_low_cardinality": is_low_cardinality,
                    "distinct_values": ", ".join(str(v) for v in distinct_values)
                    if is_low_cardinality
                    else None,
                }
            )

    return results


def update_low_cardinality_column_tags(
    spark: SparkSession, catalog_name: str, schema_name: str, tables: list[str], threshold: int = 10
) -> None:
    """Tags each column with `has_low_cardinality` (carrying the distinct values) or clears the
    tag, based on `identify_low_cardinality_columns`. Low-cardinality columns also have
    `has_check`/`has_comment` cleared so they're re-evaluated by the check/comment generators.
    """
    for obj in identify_low_cardinality_columns(
        spark, catalog_name, schema_name, tables, threshold
    ):
        table = f"{obj['table_catalog']}.{obj['table_schema']}.{obj['table_name']}"
        column = obj["column_name"]

        if obj["is_low_cardinality"]:
            set_column_tag(spark, table, column, "has_low_cardinality", obj["distinct_values"])
            unset_column_tag(spark, table, column, "has_check")
            unset_column_tag(spark, table, column, "has_comment")
        else:
            unset_column_tag(spark, table, column, "has_low_cardinality")
