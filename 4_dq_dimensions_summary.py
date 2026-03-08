# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialize the config

# COMMAND ----------

!pip install soda-core-spark-df==3.1.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

config_table = "demo.data_quality.dq_config"

# COMMAND ----------

from typing import Dict, Any
from utils.create_assets import *
from utils.soda_check_execution import *
from utils.columns_metadata import *
from utils.tables_metadata import *

# COMMAND ----------

config_data = get_configurations(config_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stewardship

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table in retrieve_table_metadata(catalog_name, schema_name, list(tables.keys())):
            metrics = {
                'has_a_valid_owner ': table['has_a_valid_owner'], 
                'is_delta_table': table['is_delta_table'],
                'uses_a_production_pipeline': table['uses_a_production_pipeline'],
                'has_enforced_retention_duration': table['has_enforced_retention_duration'],
                'is_managed_location': table['Is_managed_location'],
                'table_description': table['has_a_table_description']
            }
            upsert_table_dq_summary(catalog_name, schema_name, table['Table'], metrics)
            print('{} - {} - {} - {}'.format(catalog_name, schema_name, table['Table'], metrics))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usability

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table, columns in retrieve_columns_metadata(catalog_name, schema_name, list(tables.keys())).items():
                columns_description = True
                for column in columns:
                    if column['comment'] == '':
                        columns_description = False
                        break
                metrics = {
                    'columns_description ': columns_description
                }
                upsert_table_dq_summary(catalog_name, schema_name, table.split('.')[-1], metrics)
                print('{} - {} - {} - {}'.format(catalog_name, schema_name, table.split('.')[-1], metrics))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Freshness

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            freshness_data = check_table_freshness(
                catalog_name,
                schema_name,
                [table_name],
                params["freshness_window"],
            )
            metrics = {
                'is_fresh  ': freshness_data[0]['is_data_fresh']
            }
            upsert_table_dq_summary(catalog_name, schema_name, table_name, metrics)
            print('{} - {} - {} - {}'.format(catalog_name, schema_name, table_name, metrics))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validity

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            freshness_data = compare_table_schema_with_version(
                catalog_name,
                schema_name,
                [table_name],
                params["validity_window"],
            )
            metrics = {
                'columns_valid': freshness_data[0]['columns_match'],
                'columns_datatype_valid': freshness_data[0]['datatypes_match'] 
            }
            upsert_table_dq_summary(catalog_name, schema_name, table_name, metrics)
            print('{} - {} - {} - {}'.format(catalog_name, schema_name, table_name, metrics))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accuracy

# COMMAND ----------

for catalog_name, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        # Retrieve and zip soda check results and metadata for each table in this schema
        soda_results = execute_soda_checks(catalog_name, schema_name, list(tables.keys()))
        table_metadata = retrieve_table_metadata(catalog_name, schema_name, list(tables.keys()))

        for soda_table, metadata in zip(soda_results, table_metadata):
            # Prepare the metrics dictionary using soda check results and metadata
            metrics = {
                "table_implement_checks": metadata["table_implement_checks"],
                "all_columns_have_checks": metadata["all_columns_have_checks"],
                "has_check_passed": soda_table["all_checks_passed"],
            }
            
            # Upsert the table's DQ summary
            upsert_table_dq_summary(
                catalog_name, schema_name, soda_table["table_name"].split(".")[-1], metrics
            )
            print(
                "{} - {} - {} - {}".format(
                    catalog_name, schema_name, soda_table["table_name"].split(".")[-1], metrics
                )
            )

