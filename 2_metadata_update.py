# Databricks notebook source
# MAGIC %pip install -U --quiet databricks-sdk==0.28.0 databricks-agents mlflow-skinny mlflow mlflow[gateway] langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./generators/column_description_generator

# COMMAND ----------

from typing import Dict, List, Any
from utils.create_assets import get_configurations
from utils.columns_metadata import *
from utils.tables_metadata import *

# COMMAND ----------

config_table = "demo.data_quality.dq_config"

# COMMAND ----------

config_data = get_configurations(config_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sync the check tags

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        update_checks_tag(
            catalog_name, 
            schema_name, 
            list(tables.keys())
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update low cardinality columns

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            update_low_cardinality_column_tags(
                catalog_name,
                schema_name,
                [table_name],
                params["low_cardinality_threshold"],
            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update/generate column comments

# COMMAND ----------

def generate_column_comments(columns_metadata: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Generates and updates comments for columns in each table based on provided metadata.

    Iterates over each table and column within `columns_metadata`. If a column lacks the "has_comment" tag, an AI-generated description is created and added as a comment to the column. The column's metadata is updated with the new comment.

    Args:
        columns_metadata (dict): A dictionary where each key is a table identifier (e.g., "catalog.schema.table"),and each value is a list of column metadata dictionaries. Each dictionary includes:
            - 'column_name': str - Name of the column.
            - 'tag_names': List[str] - Tags associated with the column.

    Returns:
        None
    """
    for table, columns in columns_metadata.items():
        for column in columns:
            if 'has_comment' not in column['tag_names']:
                description = generator.invoke(
                            {"question": column, "context": ""}
                        )
                column['comment'] = description
                update_column_description(table, column['column_name'], column['comment'])
        

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        all_column_metadata = retrieve_columns_metadata(catalog_name, schema_name, list(tables.keys()))
        generate_column_comments(all_column_metadata)

# COMMAND ----------


