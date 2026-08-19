# Databricks notebook source
# MAGIC %md
# MAGIC # LakeScore: Initialize configuration
# MAGIC Seeds `dq_config` with default thresholds for every table in the monitored catalog.
# MAGIC Run first — see `resources/lakescore_job.yml` for the full pipeline order.

# COMMAND ----------

# MAGIC %pip install -U --quiet -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from lakescore.catalog.dq_config import update_table_dq_conf_parameters
from lakescore.metadata.tables import list_schemas, list_tables

# COMMAND ----------

dbutils.widgets.text("catalog_name", "demo")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# Sets default values for low_cardinality_threshold (10), freshness_window (1d), validity_window (1d)
for schema in list_schemas(
    spark,
    catalog_name,
    except_schemas=["models", "models_monitoring", "analytics", "curated", "raw"],
):
    for table in list_tables(spark, catalog_name, schema):
        update_table_dq_conf_parameters(spark, catalog_name, schema, table)

# COMMAND ----------

# For custom configuration on a specific table:
#
# update_table_dq_conf_parameters(
#     spark, catalog_name, schema_name, table_name,
#     low_cardinality_threshold=10, freshness_window="1d", validity_window="1d", tag=None,
# )
