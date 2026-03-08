# Databricks notebook source
# MAGIC %pip install -U --quiet databricks-sdk==0.28.0 databricks-agents mlflow-skinny mlflow mlflow[gateway] databricks-vectorsearch langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./generators/column_check_generator

# COMMAND ----------

from typing import Dict, List, Any
from utils.create_assets import *
from utils.columns_metadata import _set_column_check_tag, retrieve_columns_metadata, update_column_checks

# COMMAND ----------

config_table = "demo.data_quality.dq_config"

# COMMAND ----------

config_data = get_configurations(config_table)

# COMMAND ----------

def generate_column_checks(columns_metadata: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Generates and applies data quality checks for columns in each table based on provided metadata.

    Iterates over each table and column within `columns_metadata`. If a column lacks the "has_check" tag, 
    an AI-generated check is created and applied, and the column's tag is updated accordingly.

    Args:
        columns_metadata (dict): Dictionary where each key is a table identifier (e.g., "catalog.schema.table"),and each value is a list of column metadata dictionaries. Each dictionary includes:
            - 'column_name': str - Name of the column.
            - 'tag_names': List[str] - Tags associated with the column.
    
    Returns:
        None
    """
    for table, columns in all_column_metadata.items():
        for column in columns:
            if 'has_check' not in column['tag_names']:
                check = rag_generator.invoke(
                    {"messages": [{"role": "user", "content": column}]}
                )
                print("{}".format(check))
                update_column_checks(
                    table.split('.')[0],
                    table.split('.')[1],
                    table.split('.')[2],
                    column['column_name'],
                    check,
                    tag="ai_generated"
                )
                # Update tag
                _set_column_check_tag(table, column['column_name'])
        

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        all_column_metadata = retrieve_columns_metadata(catalog_name, schema_name, list(tables.keys()))
        generate_column_checks(all_column_metadata)

# COMMAND ----------


