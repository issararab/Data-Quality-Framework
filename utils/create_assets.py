from databricks.sdk.runtime import *
import os
import yaml
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from collections import defaultdict
from typing import Dict, List, Any

def get_configurations(config_table: str)-> Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]:
    """
    Fetches configuration data from a specified Spark table and structures it as a nested dictionary.

    The resulting dictionary has a structure as follows:
    {
        "catalog_name": {
            "schema_name": {
                "table_name": {
                    "low_cardinality_threshold": int,
                    "freshness_window": int,
                    "validity_window": int
                }
            }
        }
    }

    Each level (catalog, schema, table) is nested based on the row data retrieved from the Spark table.

    Parameters:
        - config_table (str): The name of the Spark table containing the configuration data.

    Returns:
        dict: Nested dictionary with configurations organized by catalog, schema, and table name.
    """
    config_data = spark.read.table(f"{config_table}").collect()
    # Nested dictionary
    config_dict = {}

    for row in config_data:
        catalog_name = row["catalog_name"]
        schema_name = row["schema_name"]
        table_name = row["table_name"]

        # Create nested structure
        if catalog_name not in config_dict:
            config_dict[catalog_name] = {}
        if schema_name not in config_dict[catalog_name]:
            config_dict[catalog_name][schema_name] = {}

        # Store the parameters
        config_dict[catalog_name][schema_name][table_name] = {
            "low_cardinality_threshold": row["low_cardinality_threshold"],
            "freshness_window": row["freshness_window"],
            "validity_window": row["validity_window"],
        }
    return config_dict


def get_checks_target_table(catalog_name: str) -> str:
    """
    Ensures the existence of the `data_quality` schema and the `column_checks` table in the specified catalog, creating them if they do not exist.

    This function first sets the current catalog to the specified `catalog_name`. It checks for the existence of the `data_quality` schema and creates it if necessary.
    Then, it verifies the presence of the `column_checks` table within the `data_quality` schema. If the table does not exist, it creates the table with specified columns,
    sets it to use the Delta format, and enables column defaults, setting the 'tag' column to NULL by default.

    Parameters:
        catalog_name (str): The name of the catalog to use and where the schema and table should be checked or created.

    Returns:
        str: The fully qualified path of the `column_checks` table in the format "{catalog_name}.data_quality.column_checks".
    """
    # Set the current catalog to the given catalog_name
    spark.sql(f"USE CATALOG {catalog_name}")

    # Check if schema "data_quality" exists
    schemas_df = spark.sql('SHOW DATABASES')
    schemas = [row.databaseName for row in schemas_df.collect()]
    if 'data_quality' not in schemas:
        # Create schema data_quality
        spark.sql("CREATE SCHEMA `data_quality`")

    # Check if table "column_checks" exists in schema "data_quality"
    tables_df = spark.sql('SHOW TABLES IN data_quality')
    tables = [row.tableName for row in tables_df.collect()]
    if 'column_checks' not in tables:
        # Create the table with the specified columns
        create_table_sql = f"""
        CREATE TABLE {catalog_name}.data_quality.column_checks (
            check_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            check STRING,
            tag STRING
        )
        USING DELTA
        """
        spark.sql(create_table_sql)

        # Enable the column default feature for this table
        spark.sql(
            f"""
                ALTER TABLE {catalog_name}.data_quality.column_checks
                SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
            """
        )

        # Update the table to set the default value for the 'tag' column to NULL
        spark.sql(
            f"""
                ALTER TABLE {catalog_name}.data_quality.column_checks
                ALTER COLUMN tag SET DEFAULT NULL
            """
        )

    # Return the path to the table
    table_path = f"{catalog_name}.data_quality.column_checks"
    return table_path



def get_dq_config_target_table(catalog_name: str) -> str:
    """
    Ensures the existence of the `data_quality` schema and the `dq_config` table in the specified catalog, creating them if they do not exist.

    This function first sets the current catalog to the specified `catalog_name`. It checks for the existence of the `data_quality` schema and creates it if necessary.
    Then, it verifies the presence of the `dq_config` table within the `data_quality` schema. If the table does not exist, it creates the table with specified columns,
    sets it to use the Delta format, and enables column defaults, setting the 'tag' column to NULL by default.

    Parameters:
        catalog_name (str): The name of the catalog to use and where the schema and table should be checked or created.

    Returns:
        str: The fully qualified path of the `dq_config` table in the format "{catalog_name}.data_quality.dq_config".
    """
    # Set the current catalog to the given catalog_name
    spark.sql(f"USE CATALOG {catalog_name}")

    # Check if schema "data_quality" exists
    schemas_df = spark.sql("SHOW DATABASES")
    schemas = [row.databaseName for row in schemas_df.collect()]
    if "data_quality" not in schemas:
        # Create schema data_quality
        spark.sql("CREATE SCHEMA `data_quality`")

    # Check if table "dq_config" exists in schema "data_quality"
    tables_df = spark.sql("SHOW TABLES IN data_quality")
    tables = [row.tableName for row in tables_df.collect()]
    if "dq_config" not in tables:
        # Create the table with the specified columns
        create_table_sql = f"""
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
        spark.sql(create_table_sql)

        # Enable the column default feature for this table
        spark.sql(
            f"""
                ALTER TABLE {catalog_name}.data_quality.dq_config
                SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
            """
        )

        # Update the table to set the default value for the 'tag' column to NULL
        spark.sql(
            f"""
                ALTER TABLE {catalog_name}.data_quality.dq_config
                ALTER COLUMN tag SET DEFAULT NULL
            """
        )

    # Return the path to the table
    table_path = f"{catalog_name}.data_quality.dq_config"
    return table_path


def update_table_dq_conf_parameters(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    low_cardinality_threshold: int = 10,
    freshness_window: str = "1d",
    validity_window: str = "1d",
    tag: str = None) -> bool:
    """
    Updates or inserts a configuration record in the `dq_config` table within the specified catalog for a given table.
    
    This function checks if a record with matching `catalog_name`, `schema_name`, and `table_name` already exists in the `dq_config` table. If it does, the record is updated with the provided parameters. If it does not exist, a new record is inserted. The function uses a MERGE INTO operation to ensure efficient upserts. If the table is empty, the function directly appends the new record.

    Parameters:
        catalog_name (str): The name of the catalog where the `dq_config` table resides.
        schema_name (str): The schema name of the target table.
        table_name (str): The name of the target table for which configurations are set.
        low_cardinality_threshold (int, optional): Threshold for low cardinality, default is 10.
        freshness_window (str, optional): Window for data freshness, default is "1d".
        validity_window (str, optional): Window for data validity, default is "1d".
        tag (str, optional): An optional tag for categorizing or identifying the configuration.

    Returns:
        bool: True if the operation is successful, False if an error occurs.
    """
    target_table = get_dq_config_target_table(catalog_name)

    # Define the schema explicitly to avoid data type inference issues
    schema = StructType(
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

    # Create a DataFrame with the new record using the explicit schema
    new_data = [
        (
            catalog_name,
            schema_name,
            table_name,
            low_cardinality_threshold,
            freshness_window,
            validity_window,
            tag,
        )
    ]

    new_df = spark.createDataFrame(new_data, schema=schema)

    # Create a temporary view for the new data
    new_df.createOrReplaceTempView("new_confs")

    # Perform the MERGE INTO operation
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

    # Check if the table exists and append or merge data accordingly
    if spark.table(f"{catalog_name}.data_quality.dq_config").count() == 0:
        try:
            new_df.write.option("mergeSchema", "true").format("delta").mode(
                "append"
            ).saveAsTable(f"{catalog_name}.data_quality.dq_config")
            return True
        except Exception as e:
            print(f"Error while executing the write operation: {e}")
            return False
    else:
        try:
            spark.sql(merge_sql)
            return True
        except Exception as e:
            print(f"Error while executing the merge statement: {e}")
            return False


def get_dq_summary_target_table(catalog_name: str) -> str:
    """
    Ensures the existence of the `data_quality` schema and the `dq_summary` table in the specified catalog, creating them if they do not exist.

    This function sets the current catalog to `catalog_name`, then checks if the `data_quality` schema and `dq_summary` table exist within it.
    If the schema or table is missing, it creates them. The `dq_summary` table includes columns tracking various data quality dimensions and is created
    with default values of `False` for boolean columns, representing checks for table and column descriptions, owner validation, Delta table format, 
    pipeline usage, retention policies, freshness, column checks, and other criteria.

    Parameters:
        catalog_name (str): The name of the catalog in which the `data_quality.dq_summary` table should be created or verified.

    Returns:
        str: The fully qualified path to the `dq_summary` table in the format "{catalog_name}.data_quality.dq_summary".
    """
        
    # Set the current catalog to the given catalog_name
    spark.sql(f"USE CATALOG {catalog_name}")

    # Check if schema "data_quality" exists
    schemas_df = spark.sql('SHOW DATABASES')
    schemas = [row.databaseName for row in schemas_df.collect()]
    if 'data_quality' not in schemas:
        # Create schema data_quality
        spark.sql("CREATE SCHEMA `data_quality`")

    # Check if table "dq_summary" exists in schema "data_quality"
    tables_df = spark.sql('SHOW TABLES IN data_quality')
    tables = [row.tableName for row in tables_df.collect()]
    if 'dq_summary' not in tables:
        # Create the table with the specified columns
        create_table_sql = f"""
        CREATE TABLE {catalog_name}.data_quality.dq_summary (
            table_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            table_description BOOLEAN,
            columns_description BOOLEAN,
            has_a_valid_owner BOOLEAN,
            is_delta_table BOOLEAN,
            uses_a_production_pipeline BOOLEAN,
            has_enforced_retention_duration BOOLEAN,
            is_managed_location BOOLEAN,
            is_fresh BOOLEAN,
            columns_valid BOOLEAN,
            columns_datatype_valid BOOLEAN,
            all_columns_have_checks BOOLEAN,
            table_implement_checks BOOLEAN,
            has_check_passed BOOLEAN
        )
        USING DELTA
        """
        spark.sql(create_table_sql)

        # Enable the column default feature for this table
        spark.sql(
            f"""
                ALTER TABLE {catalog_name}.data_quality.dq_summary
                SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
            """
        )

        # Set default value for boolean columns to 'False'
        boolean_columns = [
            'table_description', 'columns_description', 'has_a_valid_owner', 
            'is_delta_table', 'uses_a_production_pipeline', 
            'has_enforced_retention_duration', 'is_managed_location',
            'is_fresh', 'columns_valid', 'columns_datatype_valid', 
            'all_columns_have_checks', 'all_tables_have_checks', 'has_check_passed'
        ]
        for column in boolean_columns:
            spark.sql(
                f"""
                    ALTER TABLE {catalog_name}.data_quality.dq_summary
                    ALTER COLUMN {column} SET DEFAULT False
                """
            )

    # Return the path to the table
    table_path = f"{catalog_name}.data_quality.dq_summary"
    return table_path



def upsert_table_dq_summary(catalog_name: str, schema_name: str, table_name: str, metrics: dict):
    """
    Upserts data into the `dq_summary` table under the `data_quality` schema, updating only the specified metrics or inserting a new record if none exists.

    This function ensures the existence of the `dq_summary` table in the specified catalog and then performs an upsert operation. If a record with matching `catalog_name`, `schema_name`, and `table_name` already exists, only the columns in `metrics` are updated. If no such record exists, a new one is inserted with 
    the specified metrics.

    Parameters:
        catalog_name (str): The catalog in which the `dq_summary` table resides.
        schema_name (str): The schema name of the target table for which metrics are being recorded.
        table_name (str): The name of the target table for which metrics are being recorded.
        metrics (dict): A dictionary of data quality metrics, where keys are column names in the `dq_summary` table, and values are the corresponding metric values.

    Returns:
        None: This function does not return a value but updates or inserts a record in the `dq_summary` table.
    """
    # Ensure the dq_summary table exists
    dq_summary_table_path = get_dq_summary_target_table(catalog_name)

    # Dynamically construct the UPDATE SET part of the SQL query based on the provided metrics
    update_columns = ', '.join([f"{key} = {value}" for key, value in metrics.items()])

    # Prepare the MERGE INTO statement
    merge_sql = f"""
    MERGE INTO {dq_summary_table_path} AS target
    USING (SELECT '{catalog_name}' AS catalog_name, 
                  '{schema_name}' AS schema_name, 
                  '{table_name}' AS table_name) AS source
    ON target.catalog_name = source.catalog_name AND target.schema_name = source.schema_name AND target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET
        {update_columns}
    WHEN NOT MATCHED THEN INSERT (
        catalog_name, schema_name, table_name, {', '.join(metrics.keys())}
    ) VALUES (
        '{catalog_name}', '{schema_name}', '{table_name}', {', '.join([str(value) for value in metrics.values()])}
    );
    """

    spark.sql(merge_sql)

