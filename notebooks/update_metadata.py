# Databricks notebook source
# MAGIC %md
# MAGIC # LakeScore: Update metadata
# MAGIC Tags low-cardinality columns, syncs the `has_check` tag against `column_checks`, and uses
# MAGIC GenAI to generate column descriptions where missing. Run after `init_config`.

# COMMAND ----------

# MAGIC %pip install -U --quiet -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from typing import Any

from lakescore.catalog.dq_config import get_configurations
from lakescore.generators.column_description import generator
from lakescore.metadata.columns import retrieve_columns_metadata, update_column_description
from lakescore.metadata.tags import update_checks_tag
from lakescore.quality.cardinality import update_low_cardinality_column_tags

# COMMAND ----------

dbutils.widgets.text("catalog_name", "demo")
catalog_name = dbutils.widgets.get("catalog_name")
config_table = f"{catalog_name}.data_quality.dq_config"
config_data = get_configurations(spark, config_table)

# COMMAND ----------

# MAGIC %md #### Sync the check tags

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        update_checks_tag(spark, catalog_name_, schema_name, list(tables.keys()))

# COMMAND ----------

# MAGIC %md #### Update low cardinality columns

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            update_low_cardinality_column_tags(
                spark, catalog_name_, schema_name, [table_name], params["low_cardinality_threshold"]
            )

# COMMAND ----------

# MAGIC %md #### Update/generate column comments

# COMMAND ----------


def generate_column_comments(columns_metadata: dict[str, list[dict[str, Any]]]) -> None:
    """Generates and writes an AI description for every column not yet tagged `has_comment`."""
    for table, columns in columns_metadata.items():
        for column in columns:
            if "has_comment" not in column["tag_names"]:
                description = generator.invoke({"question": column, "context": ""})
                update_column_description(spark, table, column["column_name"], description)


# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        all_column_metadata = retrieve_columns_metadata(
            spark, catalog_name_, schema_name, list(tables.keys())
        )
        generate_column_comments(all_column_metadata)
