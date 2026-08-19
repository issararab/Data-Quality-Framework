# Databricks notebook source
# MAGIC %md
# MAGIC # LakeScore: Compute summary
# MAGIC Scores every configured table across all 5 dimensions and upserts the results into
# MAGIC `dq_summary`. Run last — see `docs/quality_dimensions.md` for the scoring taxonomy.

# COMMAND ----------

# MAGIC %pip install -U --quiet -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from lakescore.catalog.dq_config import get_configurations
from lakescore.catalog.dq_summary import upsert_table_dq_summary
from lakescore.metadata.columns import retrieve_columns_metadata
from lakescore.metadata.tables import retrieve_table_metadata
from lakescore.quality.freshness import check_table_freshness
from lakescore.quality.validity import compare_table_schema_with_version
from lakescore.soda_execution import execute_soda_checks

# COMMAND ----------

dbutils.widgets.text("catalog_name", "demo")
catalog_name = dbutils.widgets.get("catalog_name")
config_table = f"{catalog_name}.data_quality.dq_config"
config_data = get_configurations(spark, config_table)

# COMMAND ----------

# MAGIC %md ### Stewardship

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table in retrieve_table_metadata(
            spark, catalog_name_, schema_name, list(tables.keys())
        ):
            metrics = {
                "has_a_valid_owner": table["has_a_valid_owner"],
                "is_delta_table": table["is_delta_table"],
                "uses_a_production_pipeline": table["uses_a_production_pipeline"],
                "has_enforced_retention_duration": table["has_enforced_retention_duration"],
                "is_managed_location": table["Is_managed_location"],
                "table_description": table["has_a_table_description"],
            }
            upsert_table_dq_summary(spark, catalog_name_, schema_name, table["Table"], metrics)
            print(f"{catalog_name_} - {schema_name} - {table['Table']} - {metrics}")

# COMMAND ----------

# MAGIC %md ### Usability

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table, columns in retrieve_columns_metadata(
            spark, catalog_name_, schema_name, list(tables.keys())
        ).items():
            columns_description = all(column["comment"] != "" for column in columns)
            metrics = {"columns_description": columns_description}
            table_name = table.split(".")[-1]
            upsert_table_dq_summary(spark, catalog_name_, schema_name, table_name, metrics)
            print(f"{catalog_name_} - {schema_name} - {table_name} - {metrics}")

# COMMAND ----------

# MAGIC %md ### Freshness

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            freshness_data = check_table_freshness(
                spark, catalog_name_, schema_name, [table_name], params["freshness_window"]
            )
            metrics = {"is_fresh": freshness_data[0]["is_data_fresh"]}
            upsert_table_dq_summary(spark, catalog_name_, schema_name, table_name, metrics)
            print(f"{catalog_name_} - {schema_name} - {table_name} - {metrics}")

# COMMAND ----------

# MAGIC %md ### Validity

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        for table_name, params in tables.items():
            validity_data = compare_table_schema_with_version(
                spark, catalog_name_, schema_name, [table_name], params["validity_window"]
            )
            metrics = {
                "columns_valid": validity_data[0]["columns_match"],
                "columns_datatype_valid": validity_data[0]["datatypes_match"],
            }
            upsert_table_dq_summary(spark, catalog_name_, schema_name, table_name, metrics)
            print(f"{catalog_name_} - {schema_name} - {table_name} - {metrics}")

# COMMAND ----------

# MAGIC %md ### Accuracy

# COMMAND ----------

for catalog_name_, schemas in config_data.items():
    for schema_name, tables in schemas.items():
        # execute_soda_checks skips tables with no checks defined yet (and any whose scan
        # raised), so it can return fewer rows than table_metadata: look results up by table
        # name rather than zipping positionally, which would silently misalign the rest.
        soda_results_by_table = {
            r["table_name"].split(".")[-1]: r
            for r in execute_soda_checks(spark, catalog_name_, schema_name, list(tables.keys()))
        }

        for metadata in retrieve_table_metadata(
            spark, catalog_name_, schema_name, list(tables.keys())
        ):
            table_name = metadata["Table"]
            metrics = {
                "table_implement_checks": metadata["table_implement_checks"],
                "all_columns_have_checks": metadata["all_columns_have_checks"],
            }
            soda_result = soda_results_by_table.get(table_name)
            if soda_result is not None:
                metrics["has_check_passed"] = soda_result["all_checks_passed"]

            upsert_table_dq_summary(spark, catalog_name_, schema_name, table_name, metrics)
            print(f"{catalog_name_} - {schema_name} - {table_name} - {metrics}")
