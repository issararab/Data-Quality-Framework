from databricks.sdk.runtime import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json
import ast
from pyspark.sql import Row
from pyspark.sql.types import StructType
import re

def _list_schemas(catalog_name: str, except_schemas: list = []) -> list:
    """
    Function to list all schemas in a specified catalog, except some default system and the data_quality schemas.

    Parameters:
    - catalog_name (str): The catalog name.

    Returns:
    - list: A list of schema names.
    """

    # Extend the except_schemas list with default schemas
    except_schemas.extend(["default", "information_schema", "data_quality"])
    # Construct the query to show tables in the specified catalog and schema
    query = f"SHOW SCHEMAS IN {catalog_name}"
    
    # Run the query
    schemas_df = spark.sql(query)
    
    # Filter out schemas that are in the except_schemas list
    filtered_schemas_df = schemas_df.filter(~schemas_df.databaseName.isin(except_schemas))
    
    # Convert the result into a list of schema names
    schema_list = filtered_schemas_df.select("databaseName").rdd.flatMap(lambda x: x).collect()
    
    
    return schema_list


def _list_tables(catalog_name: str, schema_name: str) -> list:
    """
    Function to list all tables in a specified catalog and schema/database, excluding temporary tables like _sqldf.

    Parameters:
    - catalog_name (str): The catalog name.
    - schema_name (str): The schema or database name.

    Returns:
    - list: A list of table names in the specified catalog and schema.
    """
    # Construct the query to show tables in the specified catalog and schema
    query = f"SHOW TABLES IN {catalog_name}.{schema_name}"
    
    # Run the query
    tables_df = spark.sql(query)
    
    # Filter out tables ending with '_sqldf', 'table_history', and 'table_metadata' and convert the result into a list of table names
    table_list = tables_df.filter(~tables_df.tableName.endswith('_sqldf') & 
                                   ~tables_df.tableName.contains('table_history') & 
                                   ~tables_df.tableName.contains('table_metadata')).select("tableName").rdd.flatMap(lambda x: x).collect()
    
    return table_list


def _get_table_full_name(catalog_name: str, schema_name: str, table_name: str) -> str:
    """
    Constructs the full table name using the catalog, schema, and table name.

    Parameters:
    - catalog_name (str): The name of the catalog.
    - schema_name (str): The name of the schema.
    - table_name (str): The name of the table.

    Returns:
    - str: The fully qualified table name in the format catalog.schema.table.
    """
    return f"{catalog_name}.{schema_name}.{table_name}"


def _fetch_table_detail(full_table_name: str) -> dict:
    """
    Fetches detailed information about a specific table.

    Parameters:
    - full_table_name (str): The full name of the table.

    Returns:
    DataFrame: containing the table's details such as id, name, description, 
            createdAt, and lastModified.
    """
    table_details_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}") \
        .select('id', 'name', 'description', 'createdAt', 'lastModified')

    return table_details_df


def _fetch_table_history(full_table_name: str) -> dict:
    """
    Fetches the latest history record for a specific table.

    Parameters:
    - full_table_name (str): The full name of the table.

    Returns:
    DataFrame: Containing the latest history record, including the timestamp and job name.
            Returns an empty dictionary if no history is found or if an error occurs.
    """
    try:
        history_df = (spark.sql(f"DESCRIBE HISTORY {full_table_name}")
                      .selectExpr("timestamp", "job AS etl_job")
                      .withColumn("name", F.lit(full_table_name))
                      .orderBy("timestamp", ascending=False)
                      .limit(1))

        return history_df
    except Exception as e:
        print(f"Error fetching history for table {full_table_name}: {str(e)}")
        return False


def _fetch_table_metadata(full_table_name: str) -> dict:
    """
    Fetches and pivots the extended metadata for a specific table.

    Parameters:
    - full_table_name (str): The full name of the table.

    Returns:
    - Dataframe containing the extended table metadata such as Catalog, Database, Table,
            Column Names, Created Time, Last Access, Created By, Location, Provider, Owner, 
            Is_managed_location, Table Properties and table name.
    """

    metadata_columns = ['Catalog', 'Database', 'Table', 'Column Names', 'Created Time', 'Last Access', 
                        'Created By', 'Location', 'Provider', 'Owner', 
                        'Is_managed_location', 'Table Properties', 'name']
    
    table_metadata_df = spark.sql(f"DESCRIBE EXTENDED {full_table_name}") \
        .filter(F.col("col_name").isin(metadata_columns)) \
        .groupBy().pivot("col_name").agg(F.first("data_type"))
    
    table_metadata_df = table_metadata_df.withColumn("name", F.lit(full_table_name))
    
    return table_metadata_df

def _fetch_checks_for_table(full_table_name: str) -> DataFrame:
    """
    Check if a specified table has implemented data quality checks on its columns.
    
    Parameters:
    - full_table_name (str): The full name of the table.
    
    Returns:
    DataFrame: Containing columns:
        - name: The full table name
        - table_implement_checks: Boolean indicating if any columns have checks
        - all_columns_have_checks: Boolean indicating if all columns have checks
    """
    # Query to retrieve columns that have checks implemented
    columns_to_check_query = f"""
    SELECT column_name 
    FROM demo.data_quality.column_checks
    WHERE concat(catalog_name,'.',schema_name, '.', table_name ) = '{full_table_name}'
    """

    # Execute the query to get the list of columns with checks
    columns_to_check_list = (
        spark.sql(columns_to_check_query)
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Check if any columns have checks implemented
    table_implement_checks = len(columns_to_check_list) > 0

    # Get the list of columns for the specified table
    table_columns = spark.table(full_table_name).columns

    # Check if all columns have checks
    all_columns_have_checks = all(item in columns_to_check_list for item in table_columns)

    # Create a DataFrame with the results
    table_checks_metadata_df = spark.createDataFrame(
        [(full_table_name, table_implement_checks, all_columns_have_checks)],
        ["name", "table_implement_checks", "all_columns_have_checks"]
    )

    return table_checks_metadata_df


def retrieve_table_metadata(catalog_name: str, schema_name: str, list_of_tables: list) -> dict:
    """
    Retrieves metadata, history, and detailed information for a list of tables
    in a given catalog and schema, and concatenates the results into a single dictionary.

    Parameters:
    - catalog_name (str): The catalog name.
    - schema_name (str): The schema name.
    - list_of_tables (list): A list of table names to process.

    Returns:
    - dict: A dictionary containing metadata, history, and details for all tables.
    """

    final_dfs = []
    
    # Loop through each table, process and append the result
    for full_table_name in map(lambda t: _get_table_full_name(catalog_name, schema_name, t), list_of_tables):
        
        # Fetch individual components
        table_detail_df = _fetch_table_detail(full_table_name)
        table_metadata_df = _fetch_table_metadata(full_table_name)
        table_history_df = _fetch_table_history(full_table_name)
        table_checks_metadata = _fetch_checks_for_table(full_table_name)


        # Join the DataFrames together
        detail_and_metadata_df = table_detail_df.join(table_metadata_df, on='name', how='inner')
        detail_and_check_metadata_df = detail_and_metadata_df.join(table_checks_metadata, on='name', how='inner')
        final_df = detail_and_check_metadata_df.join(table_history_df, on='name', how='inner')

        # Append the resulting DataFrame to the list
        final_dfs.append(final_df)

    # Concatenate all DataFrames in the list using unionByName
    if final_dfs:
        concatenated_df = final_dfs[0]
        for df in final_dfs[1:]:
            concatenated_df = concatenated_df.unionByName(df)

        # Convert the DataFrame to a list of dictionaries
        result_dict = concatenated_df.collect()
        # Convert each row to a dictionary
        result_dict = [row.asDict() for row in result_dict] 
        
        # Selected keys to keep in the final output
        selected_keys = [
            'Catalog', 
            'Database', 
            'Table', 
            'name', 
            'id',
            'description',
            'createdAt', 
            'lastModified', 
            'has_a_valid_owner', 
            'is_delta_table', 
            'has_a_table_description', 
            'uses_a_production_pipeline', 
            'has_enforced_retention_duration',
            'Is_managed_location',
            'table_implement_checks',
            'all_columns_have_checks'

        ]
        
        for item in result_dict:
            # Check if it has a valid owner
            item['has_a_valid_owner'] = item.get('Owner') is not None
            
            # Check if it is a delta table
            item['is_delta_table'] = item.get('Provider') == 'delta'
            
            # Check if 'description' exists
            item['has_a_table_description'] = item.get('description') is not None
            
            # Check if 'etl_job' exists
            item['uses_a_production_pipeline'] = item.get('etl_job') is not None
            
            # Check if 'Table Properties' exists and is a string
            if isinstance(item.get('Table Properties'), str):
                table_properties = item['Table Properties']
                # Check for the specified substrings
                item['has_enforced_retention_duration'] = (
                    "delta.logRetentionDuration" in table_properties or 
                    "delta.deletedFileRetentionDuration" in table_properties
                )
            else:
                print(f"'Table Properties' is either missing or not a string in: {item}")  
        
        # Filter to keep only the selected keys
        filtered_result = [{key: item[key] for key in selected_keys} for item in result_dict]
        return filtered_result
    else:
        print("No tables to process.")
        return None



def _parse_threshold(threshold_str: str) -> timedelta:
    """
    Parses the freshness threshold string and converts it into a timedelta object.

    Args:
        threshold_str (str): Freshness threshold in formats like '3d', '1h', '30m', '1d6h', '1h30m'.

    Returns:
        timedelta: A timedelta object representing the freshness threshold.

    Raises:
        ValueError: If the threshold format is invalid.
    """
    # Pattern to match number and unit (e.g., '3d', '6h', '30m')
    pattern = r'(\d+)([dhm])'
    matches = re.findall(pattern, threshold_str)
    if not matches:
        raise ValueError(f"Invalid threshold format: {threshold_str}")
    days = hours = minutes = 0
    for value, unit in matches:
        if unit == 'd':
            days += int(value)
        elif unit == 'h':
            hours += int(value)
        elif unit == 'm':
            minutes += int(value)
        else:
            raise ValueError(f"Invalid time unit in threshold: {unit}")
    return timedelta(days=days, hours=hours, minutes=minutes)


def _get_schema_versioned(table_name: str, target_day: str) -> StructType:
    """
    Get the schema of the Delta table as of midnight (00:00:00) of the given day.
    If the target day is earlier than the minimum available version, return the schema of the earliest version.

    Args:
    - table_name: The path to the Delta table.
    - target_day: The target date in 'YYYY-MM-DD' format.
    
    Returns:
    - The schema of the Delta table as of the latest version on the given day or the earliest version if the target day is too early.
    """
    target_date = datetime.strptime(target_day, "%Y-%m-%d %H:%M:%S")
    target_date = target_date.replace(hour=0, minute=0, second=0)
    
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    
    # Get the minimum timestamp from the history
    min_timestamp = history_df.agg(F.min("timestamp")).collect()[0][0]
    
    # If the target date is earlier than the minimum timestamp, use the minimum timestamp
    if target_date < min_timestamp:
        earliest_version_df = (history_df
                               .orderBy(F.col("timestamp").asc())
                               .limit(1))
        earliest_version = earliest_version_df.select("version").collect()[0]["version"]
        
        # Get the schema as of the earliest version
        versioned_table_df = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {earliest_version} LIMIT 1")
        return versioned_table_df.schema
    
    # Filter the history for versions that are on or before midnight of the given day
    latest_version_df = (history_df
                         .filter(F.col("timestamp") <= target_date)
                         .orderBy(F.col("timestamp").desc())
                         .limit(1))
    
    latest_version = latest_version_df.select("version").collect()[0]["version"]
    
    # Get the schema of the Delta table as of this version
    versioned_table_df = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {latest_version} LIMIT 1")
    
    return versioned_table_df.schema



def compare_table_schema_with_version(catalog_name: str, schema_name: str, list_of_tables: list, threshold_str: str) -> dict:
    """
    Compares the schema of the current version of a list of tables with an earlier version
    from a specified time threshold.

    This function retrieves the current schema of each table, compares it to the schema
    from a previous version based on the given threshold, and returns the comparison results,
    which include column name and datatype mismatches.

    **Note**: The `threshold_str` depends on the Delta table's retention period. Delta Lake retains history for 30 days by default, but this can vary. Ensure the threshold does not exceed the retention period; otherwise, the versioned schema will reflect the earliest available date.

    Args:
        catalog_name (str): The catalog name where the tables are stored.
        schema_name (str): The schema name within the catalog.
        list_of_tables (list): A list of table names to compare.
        threshold_str (str): The time threshold to look back, in formats like '3d', '1h', '30m', '1d6h', '1h30m'.

    Returns:
        dict: A list of comparison results for each table, where each entry contains:
            - 'table_name' (str): Full name of the table.
            - 'threshold' (str): The time threshold used for the comparison.
            - 'columns_match' (bool): Whether the current and versioned schemas have the same columns.
            - 'datatypes_match' (bool): Whether the datatypes of common columns match.
            - 'mismatched_datatypes' (dict): A dictionary of columns with datatype mismatches, with the current
              and versioned datatypes.
            - 'col_missing_in_current_table' (set or None): Columns present in the versioned schema but missing
              in the current schema.
            - 'col_missing_in_old_table' (set or None): Columns present in the current schema but missing in the
              versioned schema.
    """
    comparisons_results = []
    threshold_timedelta = _parse_threshold(threshold_str)
    
    # Loop through each table, process and append the result
    for table_name in map(lambda t: _get_table_full_name(catalog_name, schema_name, t), list_of_tables):

        # Prepare current table schema
        current_data_types = {}
        for field in spark.sql(f'SELECT * FROM {table_name}').schema.fields:
            current_data_types[field.name] = str(field.dataType)

        # Prepare versioned table schema
        window_datetime = datetime.utcnow() - threshold_timedelta
        window_date_str = window_datetime.strftime('%Y-%m-%d %H:%M:%S')
        schema_versioned = _get_schema_versioned(table_name, window_date_str)
        versioned_data_types = {}
        for field in schema_versioned.fields:
            versioned_data_types[field.name] = str(field.dataType)

        # Get the set of common columns
        current_columns_set = set(current_data_types.keys())
        versioned_columns_set = set(versioned_data_types.keys())
        common_columns = current_columns_set.intersection(versioned_columns_set)
        columns_match = current_columns_set == versioned_columns_set == common_columns

        # Handle the case where the columns do not match
        if columns_match:
            missing_in_current = None
            missing_in_versioned = None
        else:
            missing_in_current = versioned_columns_set - current_columns_set
            missing_in_versioned = current_columns_set - versioned_columns_set
            columns_match = False

        # Handle the case where the datatypes are different (for common columns)
        mismatched_datatypes = {}
        for col in common_columns:
            if current_data_types[col] != versioned_data_types[col]:
                mismatched_datatypes[col] = {
                    'current': str(current_data_types[col]),
                    'versioned': str(versioned_data_types[col])
                }
        datatypes_match = len(mismatched_datatypes) == 0

        result = {
            'table_name': table_name,
            'threshold': threshold_str,
            'columns_match': columns_match,
            'datatypes_match': datatypes_match,
            'mismatched_datatypes': mismatched_datatypes,
            'col_missing_in_current_table': missing_in_current,
            'col_missing_in_old_table': missing_in_versioned
        }
        # Append the result to the list
        comparisons_results.append(result)
    return comparisons_results


def _extract_time_components(delta: timedelta) -> tuple:
    """
    Extracts days, hours, and minutes from a timedelta object.

    Args:
        delta (timedelta): The time difference to extract components from.

    Returns:
        tuple: A tuple containing (days, hours, minutes).
    """
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes = remainder // 60
    return days, hours, minutes


def check_table_freshness(catalog_name: str, schema_name: str, list_of_tables: list, freshness_threshold: str) -> dict:
    """
    Checks the freshness of specified tables by comparing their last update times against a freshness threshold.

    Args:
        catalog_name (str): The name of the catalog containing the tables.
        schema_name (str): The name of the schema containing the tables.
        list_of_tables (list): A list of table names to check for freshness.
        freshness_threshold (str): Freshness threshold in formats like '3d', '1h', '30m', '1d6h', '1h30m'.

    Returns:
        dict: A dictionary containing freshness information for each table.
              Each entry includes table name, freshness threshold, last update time,
              whether the data is fresh, and the time since the last update.
    """
    freshness_result = []

    # Parse the freshness_threshold into a timedelta interval
    interval = _parse_threshold(freshness_threshold)

    for table_name in map(lambda t: _get_table_full_name(catalog_name, schema_name, t), list_of_tables):

        # Retrieve last modification time from table history
        last_mod_time = spark.sql(f"""
            WITH history_table AS (DESCRIBE HISTORY {table_name})
            SELECT
                max(timestamp) as last_update
            FROM
                history_table
            WHERE
                operation IN (
                    'WRITE',
                    'CREATE TABLE',
                    'CREATE TABLE AS SELECT',
                    'CREATE OR REPLACE TABLE',
                    'CREATE OR REPLACE TABLE AS SELECT',
                    'COPY INTO',
                    'STREAMING UPDATE',
                    'MERGE',
                    'UPDATE',
                    'DELETE'
                )
        """).collect()[0]["last_update"]

        # Get current UTC time
        now = datetime.utcnow()

        # Get time since last update
        date_diff = now - last_mod_time

        # Extract days, hours, and minutes
        days, hours, minutes = _extract_time_components(date_diff)

        # Format time_since_last_update
        time_since_last_update = f"{days}d {hours}h {minutes}m"

        # Check if data is up to date
        is_data_fresh = date_diff < interval

        result = {
            'table_name': table_name,
            'freshness_threshold': freshness_threshold,
            'last_update': str(last_mod_time),
            'is_data_fresh': is_data_fresh,
            'time_since_last_update': time_since_last_update
        }
        freshness_result.append(result)
    
    return freshness_result