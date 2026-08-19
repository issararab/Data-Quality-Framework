"""Pure reads of table-level metadata: schema/table listings, DESCRIBE DETAIL/HISTORY/EXTENDED,
and the stewardship/usability signals derived from them.

Every public function takes an explicit `spark: SparkSession` argument rather than relying on
the notebook-injected global, so this module can run against a local Spark session in tests.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def list_schemas(
    spark: SparkSession, catalog_name: str, except_schemas: list[str] | None = None
) -> list[str]:
    """Lists schemas in `catalog_name`, excluding system schemas and any caller-supplied ones.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog to list schemas from.
        except_schemas (list[str] | None): Additional schema names to exclude.

    Returns:
        list[str]: Schema names, excluding `default`, `information_schema`, `data_quality`,
            and any names in `except_schemas`.
    """
    excluded = {"default", "information_schema", "data_quality", *(except_schemas or [])}
    schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
    return (
        schemas_df.filter(~schemas_df.databaseName.isin(excluded))
        .select("databaseName")
        .rdd.flatMap(lambda x: x)
        .collect()
    )


def list_tables(spark: SparkSession, catalog_name: str, schema_name: str) -> list[str]:
    """Lists tables in `catalog_name.schema_name`, excluding LakeScore's own history/metadata
    helper tables and Spark's transient `_sqldf` views.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog to list tables from.
        schema_name (str): The schema to list tables from.

    Returns:
        list[str]: Table names.
    """
    tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
    return (
        tables_df.filter(
            ~tables_df.tableName.endswith("_sqldf")
            & ~tables_df.tableName.contains("table_history")
            & ~tables_df.tableName.contains("table_metadata")
        )
        .select("tableName")
        .rdd.flatMap(lambda x: x)
        .collect()
    )


def full_table_name(catalog_name: str, schema_name: str, table_name: str) -> str:
    """Builds a fully qualified `catalog.schema.table` name."""
    return f"{catalog_name}.{schema_name}.{table_name}"


def _fetch_table_detail(spark: SparkSession, full_table_name_: str) -> DataFrame:
    """Runs `DESCRIBE DETAIL` for a table, projected to the columns LakeScore scores on."""
    return spark.sql(f"DESCRIBE DETAIL {full_table_name_}").select(
        "id", "name", "description", "createdAt", "lastModified"
    )


def _fetch_table_history(spark: SparkSession, full_table_name_: str) -> DataFrame | None:
    """Returns the single most recent `DESCRIBE HISTORY` row (timestamp + job name), or `None`
    if history can't be fetched (e.g. the table has none yet)."""
    try:
        return (
            spark.sql(f"DESCRIBE HISTORY {full_table_name_}")
            .selectExpr("timestamp", "job AS etl_job")
            .withColumn("name", F.lit(full_table_name_))
            .orderBy("timestamp", ascending=False)
            .limit(1)
        )
    except Exception:
        return None


def _fetch_table_metadata(spark: SparkSession, full_table_name_: str) -> DataFrame:
    """Runs `DESCRIBE EXTENDED` and pivots the fields LakeScore scores on into columns."""
    metadata_columns = [
        "Catalog",
        "Database",
        "Table",
        "Column Names",
        "Created Time",
        "Last Access",
        "Created By",
        "Location",
        "Provider",
        "Owner",
        "Is_managed_location",
        "Table Properties",
        "name",
    ]
    table_metadata_df = (
        spark.sql(f"DESCRIBE EXTENDED {full_table_name_}")
        .filter(F.col("col_name").isin(metadata_columns))
        .groupBy()
        .pivot("col_name")
        .agg(F.first("data_type"))
    )
    return table_metadata_df.withColumn("name", F.lit(full_table_name_))


def _fetch_checks_for_table(
    spark: SparkSession, catalog_name: str, full_table_name_: str
) -> DataFrame:
    """Checks whether `full_table_name_` has any/all columns covered by a check in
    `catalog_name.data_quality.column_checks`.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog owning the `data_quality.column_checks` table to query
            (must match the catalog the table itself lives in).
        full_table_name_ (str): The fully qualified table name being scored.

    Returns:
        DataFrame: Single row with `name`, `table_implement_checks`, `all_columns_have_checks`.
    """
    columns_to_check_list = (
        spark.sql(
            f"""
            SELECT column_name
            FROM {catalog_name}.data_quality.column_checks
            WHERE concat(catalog_name,'.',schema_name, '.', table_name) = '{full_table_name_}'
            """
        )
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    table_implement_checks = len(columns_to_check_list) > 0
    table_columns = spark.table(full_table_name_).columns
    all_columns_have_checks = all(col in columns_to_check_list for col in table_columns)

    return spark.createDataFrame(
        [(full_table_name_, table_implement_checks, all_columns_have_checks)],
        ["name", "table_implement_checks", "all_columns_have_checks"],
    )


def retrieve_table_metadata(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> list[dict[str, Any]] | None:
    """Retrieves and joins detail/metadata/history/check-coverage for each table, then derives
    the stewardship/usability boolean signals LakeScore scores on.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): The catalog the tables live in.
        schema_name (str): The schema the tables live in.
        list_of_tables (list[str]): Table names to process.

    Returns:
        list[dict[str, Any]] | None: One dict per table with the fields listed in
            `selected_keys` below, or `None` if `list_of_tables` is empty.
    """
    final_dfs = []
    for name in (full_table_name(catalog_name, schema_name, t) for t in list_of_tables):
        detail_df = _fetch_table_detail(spark, name)
        metadata_df = _fetch_table_metadata(spark, name)
        history_df = _fetch_table_history(spark, name)
        checks_df = _fetch_checks_for_table(spark, catalog_name, name)

        joined = detail_df.join(metadata_df, on="name", how="inner").join(
            checks_df, on="name", how="inner"
        )
        if history_df is not None:
            joined = joined.join(history_df, on="name", how="inner")
        final_dfs.append(joined)

    if not final_dfs:
        return None

    concatenated_df = final_dfs[0]
    for df in final_dfs[1:]:
        concatenated_df = concatenated_df.unionByName(df, allowMissingColumns=True)

    result_dict = [row.asDict() for row in concatenated_df.collect()]

    selected_keys = [
        "Catalog",
        "Database",
        "Table",
        "name",
        "id",
        "description",
        "createdAt",
        "lastModified",
        "has_a_valid_owner",
        "is_delta_table",
        "has_a_table_description",
        "uses_a_production_pipeline",
        "has_enforced_retention_duration",
        "Is_managed_location",
        "table_implement_checks",
        "all_columns_have_checks",
    ]

    for item in result_dict:
        item["has_a_valid_owner"] = item.get("Owner") is not None
        item["is_delta_table"] = item.get("Provider") == "delta"
        item["has_a_table_description"] = item.get("description") is not None
        item["uses_a_production_pipeline"] = item.get("etl_job") is not None

        table_properties = item.get("Table Properties")
        if isinstance(table_properties, str):
            item["has_enforced_retention_duration"] = (
                "delta.logRetentionDuration" in table_properties
                or "delta.deletedFileRetentionDuration" in table_properties
            )
        else:
            item["has_enforced_retention_duration"] = False

    return [{key: item.get(key) for key in selected_keys} for item in result_dict]
