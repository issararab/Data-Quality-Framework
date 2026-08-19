"""DDL-ensure and CRUD for the `data_quality.dq_config` table: per-table LakeScore parameters
(low-cardinality threshold, freshness/validity windows)."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lakescore.catalog._common import ensure_schema_exists, table_exists

logger = logging.getLogger(__name__)

_SCHEMA = StructType(
    [
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("low_cardinality_threshold", IntegerType(), False),
        StructField("freshness_window", StringType(), False),
        StructField("validity_window", StringType(), False),
        StructField("tag", StringType(), True),
    ]
)


def get_dq_config_target_table(spark: SparkSession, catalog_name: str) -> str:
    """Ensures `catalog_name.data_quality.dq_config` exists, creating it (with a `NULL` default
    on `tag`) if needed.

    Returns:
        str: The fully qualified table path.
    """
    ensure_schema_exists(spark, catalog_name)
    if not table_exists(spark, "data_quality", "dq_config"):
        spark.sql(
            f"""
            CREATE TABLE {catalog_name}.data_quality.dq_config (
                _id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
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
            f"ALTER TABLE {catalog_name}.data_quality.dq_config "
            "SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
        )
        spark.sql(
            f"ALTER TABLE {catalog_name}.data_quality.dq_config ALTER COLUMN tag SET DEFAULT NULL"
        )
    return f"{catalog_name}.data_quality.dq_config"


def get_configurations(
    spark: SparkSession, config_table: str
) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    """Reads `config_table` into a nested `catalog -> schema -> table -> params` dict.

    Parameters:
        spark (SparkSession): Active Spark session.
        config_table (str): Fully qualified `dq_config` table path.

    Returns:
        dict: `{catalog_name: {schema_name: {table_name: {low_cardinality_threshold,
            freshness_window, validity_window}}}}`.
    """
    config_dict: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for row in spark.read.table(config_table).collect():
        catalog = config_dict.setdefault(row["catalog_name"], {})
        schema = catalog.setdefault(row["schema_name"], {})
        schema[row["table_name"]] = {
            "low_cardinality_threshold": row["low_cardinality_threshold"],
            "freshness_window": row["freshness_window"],
            "validity_window": row["validity_window"],
        }
    return config_dict


def update_table_dq_conf_parameters(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    low_cardinality_threshold: int = 10,
    freshness_window: str = "1d",
    validity_window: str = "1d",
    tag: str | None = None,
) -> bool:
    """Upserts a `dq_config` record for one table.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog owning both the target table and `dq_config`.
        schema_name (str): Schema of the target table.
        table_name (str): The target table's name.
        low_cardinality_threshold (int): Max distinct values to classify a column as
            low-cardinality. Defaults to 10.
        freshness_window (str): Freshness evaluation window (e.g. `"1d"`).
        validity_window (str): Validity evaluation window (e.g. `"1d"`).
        tag (str | None): Optional free-form tag for this configuration.

    Returns:
        bool: `True` on success, `False` if the write/merge failed.
    """
    target_table = get_dq_config_target_table(spark, catalog_name)

    new_df = spark.createDataFrame(
        [
            (
                catalog_name,
                schema_name,
                table_name,
                low_cardinality_threshold,
                freshness_window,
                validity_window,
                tag,
            )
        ],
        schema=_SCHEMA,
    )
    new_df.createOrReplaceTempView("new_confs")

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING new_confs AS source
        ON target.catalog_name = source.catalog_name
          AND target.schema_name = source.schema_name
          AND target.table_name = source.table_name
        WHEN MATCHED THEN
          UPDATE SET target.low_cardinality_threshold = source.low_cardinality_threshold,
                      target.freshness_window = source.freshness_window,
                      target.validity_window = source.validity_window,
                      target.tag = source.tag
        WHEN NOT MATCHED THEN
          INSERT (catalog_name, schema_name, table_name, low_cardinality_threshold, freshness_window, validity_window, tag)
          VALUES (source.catalog_name, source.schema_name, source.table_name, source.low_cardinality_threshold, source.freshness_window, source.validity_window, source.tag)
    """

    try:
        if spark.table(target_table).count() == 0:
            new_df.write.option("mergeSchema", "true").format("delta").mode("append").saveAsTable(
                target_table
            )
        else:
            spark.sql(merge_sql)
        return True
    except Exception as e:
        logger.error("Error while writing to %s: %s", target_table, e)
        return False
