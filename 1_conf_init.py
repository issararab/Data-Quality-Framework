# Databricks notebook source
from utils.create_assets import update_table_dq_conf_parameters
from utils.tables_metadata import _list_schemas, _list_tables

# COMMAND ----------

catalog =  "demo"
schemas = _list_schemas(catalog, except_schemas= ["models", "models_monitoring","analytics", "curated", "raw"])
# Sets default values to low_cardinality threshold (10), freshness_window (1d), and validity_window (1d)
for schema in schemas:
    for table in _list_tables(catalog, schema):
        #print(table)
        update_table_dq_conf_parameters(catalog, schema, table)

# COMMAND ----------

# For custom configurations on tables, run the command bellow
# update_table_dq_conf_parameters(catalog_name: str,
#    schema_name,
#    table_name,
#    low_cardinality_threshold = 10,
#    freshness_window = "1d",
#    validity_window = "1d",
#    tag = None)
