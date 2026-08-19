"""Pure reads of column-level metadata (information_schema-backed) plus the single mutation
that writes generated descriptions back as column comments."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakescore.metadata.tags import set_column_tag
from lakescore.sql_utils import escape_sql_string


def get_table_columns(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> str:
    """Reads `information_schema.columns` left-joined with `information_schema.column_tags`
    for the given tables.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog to query.
        schema_name (str): The schema within the catalog.
        list_of_tables (list[str]): Table names to include.

    Returns:
        str: A JSON array; one object per column, with `tag_names`/`tag_values` lists.
    """
    columns_df = spark.read.table(f"{catalog_name}.information_schema.columns")
    filtered_columns_df = columns_df.filter(
        (F.col("table_catalog") == catalog_name)
        & (F.col("table_schema") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select(
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "is_nullable",
        "data_type",
        "comment",
    )

    tags_df = spark.read.table(f"{catalog_name}.information_schema.column_tags")
    filtered_tags_df = tags_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select("catalog_name", "schema_name", "table_name", "column_name", "tag_name", "tag_value")

    joined_df = filtered_columns_df.join(
        filtered_tags_df,
        on=[
            filtered_columns_df.table_catalog == filtered_tags_df.catalog_name,
            filtered_columns_df.table_schema == filtered_tags_df.schema_name,
            filtered_columns_df.table_name == filtered_tags_df.table_name,
            filtered_columns_df.column_name == filtered_tags_df.column_name,
        ],
        how="left",
    ).select(
        filtered_columns_df["table_catalog"],
        filtered_columns_df["table_schema"],
        filtered_columns_df["table_name"],
        filtered_columns_df["column_name"],
        filtered_columns_df["is_nullable"],
        filtered_columns_df["data_type"],
        filtered_columns_df["comment"],
        filtered_tags_df["tag_name"],
        filtered_tags_df["tag_value"],
    )

    grouped_df = joined_df.groupBy(
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "is_nullable",
        "data_type",
        "comment",
    ).agg(
        F.collect_list("tag_name").alias("tag_names"),
        F.collect_list("tag_value").alias("tag_values"),
    )

    result_dict = [row.asDict() for row in grouped_df.collect()]
    return json.dumps(result_dict, indent=4)


def get_table_metadata(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> str:
    """Reads owner/comment metadata from `information_schema.tables`.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog to query.
        schema_name (str): The schema within the catalog.
        list_of_tables (list[str]): Table names to include.

    Returns:
        str: A JSON array; one object per table with catalog/schema/table/owner/comment.
    """
    tables_df = spark.read.table(f"{catalog_name}.information_schema.tables")
    filtered_df = tables_df.filter(
        (F.col("table_catalog") == catalog_name)
        & (F.col("table_schema") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select("table_catalog", "table_schema", "table_name", "table_owner", "comment")

    return json.dumps([row.asDict() for row in filtered_df.collect()], indent=4)


def retrieve_columns_metadata(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Groups `get_table_columns` output by fully qualified table name for downstream
    check/description generation.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog to query.
        schema_name (str): The schema within the catalog.
        list_of_tables (list[str]): Table names to include.

    Returns:
        dict[str, list[dict[str, Any]]]: Fully qualified table name -> list of column metadata
            dicts (`column_name`, `is_nullable`, `data_type`, `comment`, `tag_names`, `tag_values`).
    """
    result = json.loads(get_table_columns(spark, catalog_name, schema_name, list_of_tables))
    table_info: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for table in list_of_tables:
        full_name = f"{catalog_name}.{schema_name}.{table}"
        table_info[full_name] = [
            {
                "column_name": obj["column_name"],
                "is_nullable": obj["is_nullable"],
                "data_type": obj["data_type"],
                "comment": obj["comment"],
                "tag_names": obj["tag_names"],
                "tag_values": obj["tag_values"],
            }
            for obj in result
            if obj["table_name"] == table
        ]

    return dict(table_info)


def update_column_description(
    spark: SparkSession, table_path: str, column_name: str, description: str
) -> None:
    """Sets a column's `COMMENT` to `description` and marks it with the `has_comment` tag.

    Parameters:
        spark (SparkSession): Active Spark session.
        table_path (str): Fully qualified table name.
        column_name (str): The column to update.
        description (str): The (possibly LLM-generated) description text; escaped before being
            spliced into the `COMMENT` statement.
    """
    escaped_description = escape_sql_string(description)
    spark.sql(
        f"""
        ALTER TABLE {table_path}
        ALTER COLUMN {column_name}
        COMMENT "{escaped_description}";
        """
    )
    set_column_tag(spark, table_path, column_name, "has_comment")


def parse_column(column: str) -> str | None:
    """Transforms a JSON-encoded column dict (with `tag_names`/`tag_values` lists) into a
    single-tag summary, preferring the `has_low_cardinality` tag when present.

    Parameters:
        column (str): JSON string with `column_name`, `is_nullable`, `data_type`, `comment`,
            `tag_names`, `tag_values`.

    Returns:
        str | None: `str(dict)` of the transformed record, or `None` if `column` isn't valid JSON.
    """
    try:
        data = json.loads(column)
    except json.JSONDecodeError:
        return None

    tag_name = tag_value = None
    if data.get("tag_names") and data.get("tag_values"):
        try:
            index = data["tag_names"].index("has_low_cardinality")
        except ValueError:
            index = 0
        tag_name = data["tag_names"][index]
        tag_value = data["tag_values"][index]

    return str(
        {
            "column_name": data["column_name"],
            "is_nullable": data["is_nullable"],
            "data_type": data["data_type"],
            "comment": data["comment"],
            "tag_name": tag_name,
            "tag_value": tag_value,
        }
    )
