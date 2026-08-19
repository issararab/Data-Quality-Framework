# Databricks notebook source
# MAGIC %md
# MAGIC # LakeScore: Generate checks
# MAGIC RAG-generates SodaCL column checks (grounded in the SodaCL knowledge base) for every
# MAGIC column not yet tagged `has_check`. Run after `update_metadata`.

# COMMAND ----------

# MAGIC %pip install -U --quiet -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# The catalog_name widget must exist *before* the next cell's import: importing
# lakescore.generators.column_check_rag builds the RAG chain at module-load time, and that
# build reads this widget via LakeScoreConfig.from_widgets.
dbutils.widgets.text("catalog_name", "demo")

# COMMAND ----------

from typing import Any

from lakescore.catalog.column_checks import update_column_checks
from lakescore.catalog.dq_config import get_configurations
from lakescore.generators.column_check_rag import rag_generator
from lakescore.metadata.columns import retrieve_columns_metadata
from lakescore.metadata.tags import set_column_tag

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
config_table = f"{catalog_name}.data_quality.dq_config"
config_data = get_configurations(spark, config_table)

# COMMAND ----------


def generate_column_checks(columns_metadata: dict[str, list[dict[str, Any]]]) -> None:
    """Generates and stores a RAG check for every column not yet tagged `has_check`."""
    for table, columns in columns_metadata.items():
        for column in columns:
            if "has_check" not in column["tag_names"]:
                check = rag_generator.invoke({"messages": [{"role": "user", "content": column}]})
                print(check)
                catalog_, schema_, table_ = table.split(".")
                update_column_checks(
                    spark,
                    catalog_,
                    schema_,
                    table_,
                    column["column_name"],
                    check,
                    tag="ai_generated",
                )
                set_column_tag(spark, table, column["column_name"], "has_check")


# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        all_column_metadata = retrieve_columns_metadata(
            spark, catalog_name_, schema_name, list(tables.keys())
        )
        generate_column_checks(all_column_metadata)
