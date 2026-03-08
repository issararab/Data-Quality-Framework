from databricks.sdk.runtime import *
import json
from collections import defaultdict
from pyspark.sql import functions as F
from pyspark.sql.functions import col, countDistinct, collect_set
from typing import Optional
from utils.create_assets import *


def _drop_column_checks(
    checks_table_path: str,
    check_ids_to_remove: list
) -> bool:
    """
    Deletes rows from the column_checks table based on the provided check_ids.

    Parameters:
    ----------
    checks_table_path : str
        The path to the column_checks table.
    check_ids_to_remove : list
        A list of check_ids to be removed.

    Returns:
    -------
    bool
        Returns True if the operation is successful, False otherwise.
    """
    
    try:
        if check_ids_to_remove:
            ids_to_remove_str = ', '.join([str(id) for id in check_ids_to_remove])

            delete_sql = f"""
                DELETE FROM {checks_table_path}
                WHERE check_id IN ({ids_to_remove_str})
            """
            spark.sql(delete_sql)

            return True

    except Exception as e:
        print(f"Error while executing SQL DELETE: {e}")
        return False




def _sync_checks_target_table(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> bool:
    """
    Synchronizes the column checks in the specified target table by identifying and 
    removing entries with empty checks or non-existing columns for the given tables.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog (database) to query.
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to synchronize column checks.

    Returns:
    -------
    bool
        Returns True if the deletion operation is successful, False otherwise.
    """

    # List of check_ids to remove
    check_ids_to_remove = []
    
    # Get the target table path for column checks
    checks_table_path = get_checks_target_table(catalog_name)

    # Read the `catalog_name.data_quality.column_checks` table
    checks_df = spark.read.table(checks_table_path)

    ## Get entries with empty checks
    # Get rows with empty checks
    empty_checks = checks_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
        & (F.col("check").isNull()) 
    ).select("check_id").distinct().rdd.flatMap(lambda x: x).collect()

    check_ids_to_remove.extend(empty_checks)

    ## Get entries for non-exisitng columns in the warehouse
    # Read the `<catalog_name>.data_quality.column_checks` table
    checks_df = spark.read.table(checks_table_path)

    # Filter the checks table for the given catalog, schema, and tables,
    filtered_checks_df = checks_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
        & (F.col("check").isNotNull()) 
    ).select(
        "check_id",
        "catalog_name",
        "schema_name",
        "table_name",
        "column_name"
    ).distinct()

    # Read the `information_schema.columns` table
    columns_df = spark.read.table(f"{catalog_name}.information_schema.columns")

    # Filter the columns table
    filtered_columns_df = columns_df.filter(
        (F.col("table_catalog") == catalog_name)
        & (F.col("table_schema") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select(
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "data_type"
    ).distinct()


    # Perform a left join
    checks_for_non_existing_columns = filtered_checks_df.join(
        filtered_columns_df,
        on=[
            filtered_columns_df.table_catalog == filtered_checks_df.catalog_name,
            filtered_columns_df.table_schema == filtered_checks_df.schema_name,
            filtered_columns_df.table_name == filtered_checks_df.table_name,
            filtered_columns_df.column_name == filtered_checks_df.column_name
        ],
        how="left"
    ).filter(
        (F.col("data_type").isNull())
    ).select("check_id").rdd.flatMap(lambda x: x).collect()

    check_ids_to_remove.extend(checks_for_non_existing_columns)

    return _drop_column_checks(checks_table_path,check_ids_to_remove)



def _get_columns_with_valid_checks(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> dict:
    """
    Updates and retrieves column checks for tables in the given schema and catalog.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query.
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query column checks.

    Returns:
    -------
    dict
        A dictionary where the key is "catalog_name.schema_name.table_name"
        and the value is a list of columns that have checks.
    """
    # Get the target table path for column checks
    checks_table_path = get_checks_target_table(catalog_name)

    # Read the `catalog_name.data_quality.column_checks` table
    tags_df = spark.read.table(checks_table_path)

    # Filter the checks table for the given catalog, schema, and tables,
    filtered_tags_df = tags_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
        & (F.col("check").isNotNull())  # Only include columns with checks
    ).select(
        "catalog_name",
        "schema_name",
        "table_name",
        "column_name"
    ).distinct()  

    # Get columns which already have a check
    grouped_df = filtered_tags_df.groupBy(
        "catalog_name", "schema_name", "table_name"
    ).agg(
        F.collect_list("column_name").alias("columns")
    )

    # Convert the result to a dictionary
    result_dict = grouped_df.rdd.map(
        lambda row: (f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}", row['columns'])
    ).collectAsMap()

    return result_dict




def _get_columns_with_invalid_check_tags(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> dict:
    """
    Updates and retrieves column checks for tables in the given schema and catalog.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query.
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query column checks.

    Returns:
    -------
    dict
        A dictionary where the key is "catalog_name.schema_name.table_name"
        and the value is a list of columns that have checks.
    """
    # Get the target table path for column checks
    checks_table_path = get_checks_target_table(catalog_name)

    # Read the `catalog_name.data_quality.column_checks` table
    checks_df = spark.read.table(checks_table_path)

    # Filter the checks table for the given catalog, schema, and tables,
    filtered_checks_df = checks_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
        & (F.col("check").isNotNull())  # Only include columns with checks
    ).select(
        "catalog_name",
        "schema_name",
        "table_name",
        "column_name",
        "check"
    ).distinct()  

    # Read the `information_schema.column_tags` table
    tags_df = spark.read.table(f"{catalog_name}.information_schema.column_tags")

    # Filter the tags table
    filtered_tags_df = tags_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("tag_name") == "has_check")
        & (F.col("table_name").isin(list_of_tables))
    ).select(
        "catalog_name",
        "schema_name",
        "table_name",
        "column_name"
    )

    # Perform a left join
    joined_df = filtered_tags_df.join(
        filtered_checks_df,
        on=[
            filtered_checks_df.catalog_name == filtered_tags_df.catalog_name,
            filtered_checks_df.schema_name == filtered_tags_df.schema_name,
            filtered_checks_df.table_name == filtered_tags_df.table_name,
            filtered_checks_df.column_name == filtered_tags_df.column_name
        ],
        how="left"
    ).filter(
        (F.col("check").isNull())
    ).select(
        filtered_tags_df["catalog_name"],
        filtered_tags_df["schema_name"],
        filtered_tags_df["table_name"],
        filtered_tags_df["column_name"]
    )

    # Get columns which already have a check
    grouped_df = joined_df.groupBy(
        "catalog_name", "schema_name", "table_name"
    ).agg(
        F.collect_list("column_name").alias("columns")
    )

    # Convert the result to a dictionary
    result_dict = grouped_df.rdd.map(
        lambda row: (f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}", row['columns'])
    ).collectAsMap()

    return result_dict




def _set_column_check_tag(table: str, column: str) -> bool:
    """
    Applies a 'has_check' tag to the specified column in the given table.

    :param table: The name of the table containing the column to tag.
    :param column: The column to which the 'has_check' tag should be applied.
    :return: True if the tag is successfully set; False if an error occurs.
    """
    try:
        update_tag_query = f"""
            ALTER TABLE
                {table}
            ALTER COLUMN
                {column} SET 
                TAGS ('has_check');
        """
        spark.sql(update_tag_query)     
        return True

    except Exception as e:
        print(f"Error while executing SQL ALTER SET TAG: {e}")
        return False

def _unset_column_check_tag(table: str, column: str) -> bool:
    """
    Removes the 'has_check' tag from the specified column in the given table.

    :param table: The name of the table containing the column to untag.
    :param column: The column from which the 'has_check' tag should be removed.
    :return: True if the tag is successfully removed; False if an error occurs.
    """
    try:
        update_tag_query = f"""
            ALTER TABLE
                {table}
            ALTER COLUMN
                {column} UNSET 
                TAGS ('has_check');
        """
        spark.sql(update_tag_query)     
        return True

    except Exception as e:
        print(f"Error while executing SQL ALTER UNSET TAG: {e}")
        return False



def update_checks_tag(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> dict:
    """
    Updates and retrieves column checks for tables in the given schema and catalog.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query.
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query column checks.

    Returns:
    -------
    dict
        A dictionary where the key is "catalog_name.schema_name.table_name"
        and the value is a list of columns that have checks.
    """
    # First, sync the checks table
    _sync_checks_target_table(catalog_name, schema_name, list_of_tables)

    # Update invalid check tags
    invalid_check_tags = _get_columns_with_invalid_check_tags(
        catalog_name, schema_name, list_of_tables
    )

    for table, columns in invalid_check_tags.items():
        for column in columns:
            _unset_column_check_tag(table, column)


    # Update valid check tags
    invalid_check_tags = _get_columns_with_valid_checks(
        catalog_name, schema_name, list_of_tables
    )

    for table, columns in invalid_check_tags.items():
        for column in columns:
            _set_column_check_tag(table, column)
    return "Check tags updated!"
    



def get_table_columns(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> dict:
    """
    Queries the information_schema.columns table and performs a left join with information_schema.column_tags.
    Returns relevant column information, including any tags for the columns in dictionary/JSON format.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query (e.g., the database).
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query column information.

    Returns:
    -------
    dict
        A dictionary (or JSON-like structure) where each row of the result is represented as an object,
        containing the table catalog, table schema, table name, column name, and column tags (if any).
    """

    # Read the `information_schema.columns` table
    columns_df = spark.read.table(f"{catalog_name}.information_schema.columns")

    # Filter the columns table
    filtered_columns_df = columns_df.filter(
        (F.col("table_catalog") == catalog_name)
        & (F.col("table_schema") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select(
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "is_nullable",
        "data_type",
        "comment"
    )

    # Read the `information_schema.column_tags` table
    tags_df = spark.read.table(f"{catalog_name}.information_schema.column_tags")

    # Filter the tags table
    filtered_tags_df = tags_df.filter(
        (F.col("catalog_name") == catalog_name)
        & (F.col("schema_name") == schema_name)
        & (F.col("table_name").isin(list_of_tables))
    ).select(
        "catalog_name",
        "schema_name",
        "table_name",
        "column_name",
        "tag_name",
        "tag_value"
    )

    # Perform a left join
    joined_df = filtered_columns_df.join(
        filtered_tags_df,
        on=[
            filtered_columns_df.table_catalog == filtered_tags_df.catalog_name,
            filtered_columns_df.table_schema == filtered_tags_df.schema_name,
            filtered_columns_df.table_name == filtered_tags_df.table_name,
            filtered_columns_df.column_name == filtered_tags_df.column_name
        ],
        how="left"
    ).select(
        filtered_columns_df["table_catalog"],
        filtered_columns_df["table_schema"],
        filtered_columns_df["table_name"],
        filtered_columns_df["column_name"],
        filtered_columns_df["is_nullable"],
        filtered_columns_df["data_type"],
        filtered_columns_df["comment"],
        filtered_tags_df["tag_name"],
        filtered_tags_df["tag_value"]
    )

    # Group by all columns except `tag_name` and `tag_value`
    grouped_df = joined_df.groupBy(
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "is_nullable",
        "data_type",
        "comment"
    ).agg(
        F.collect_list("tag_name").alias("tag_names"),
        F.collect_list("tag_value").alias("tag_values")
    )

    # Collect the results 
    result = grouped_df.collect()
    result_dict = [row.asDict() for row in result]
    result_json = json.dumps(result_dict, indent=4)

    return result_json



def get_table_metadata(
    catalog_name: str, schema_name: str, list_of_tables: list
) -> dict:
    """
    Queries the information_schema.tables table in a given catalog and schema for a list of tables,
    and returns relevant table metadata in dictionary/JSON format.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query (e.g., the database).
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query table metadata.

    Returns:
    -------
    dict
        A dictionary (or JSON-like structure) where each row of the result is represented as an object,
        containing the table catalog, table schema, table name, table owner, and table comment.

    """

    # Read the `information_schema.tables` table
    tables_df = spark.read.table(f"{catalog_name}.information_schema.tables")

    # Filter table
    filtered_df = tables_df.filter(
        (col("table_catalog") == catalog_name)
        & (col("table_schema") == schema_name)
        & (col("table_name").isin(list_of_tables))
    ).select(
        "table_catalog", 
        "table_schema", 
        "table_name", 
        "table_owner", 
        "comment")

    # Collect the results
    result = filtered_df.collect()
    result_dict = [row.asDict() for row in result]
    result_json = json.dumps(result_dict, indent=4)

    return result_json



def identify_low_cardinality_columns(
    catalog_name: str, schema_name: str, list_of_tables: list, threshold: int = 10
) -> dict:
    """
    Identifies low cardinality columns in the provided list of tables based on distinct value count.
    The columns will be tagged 'is_low_cardinality': 'Yes' or 'No'.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog.
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to check the columns for low cardinality.
    threshold : int, optional
        The threshold below which a column is considered 'low cardinality' (default is 10 distinct values).

    Returns:
    -------
    dict
        A dictionary (or JSON-like structure) where each row represents a column and its low-cardinality status.
    """

    # Get column information using the get_table_columns function
    column_info = get_table_columns(catalog_name, schema_name, list_of_tables)
    
    low_cardinality_info = []
    # Iterate over each table and column
    for table in list_of_tables:
        # Load the table data
        full_table_name = f"{catalog_name}.{schema_name}.{table}"
        table_df = spark.read.table(full_table_name)

        # Filter the columns for the current table
        table_columns = list(
            map(
                lambda obj: [obj["column_name"], obj["tag_names"]],
                filter(lambda obj: obj["table_name"] == table, json.loads(column_info)),
            )
        )

        # For each column, count the distinct values
        for column, tag_names in table_columns:
            distinct_values = (
                table_df.select(column)
                .agg(collect_set(column).alias("distinct_values"))
                .collect()[0]["distinct_values"]
            )
            
            if 'has_low_cardinality' in tag_names:
                is_low_cardinality = True
            else:
                is_low_cardinality = True if len(distinct_values) <= threshold else False

            # Update the list
            low_cardinality_info.append(
                {
                    "table_catalog": catalog_name,
                    "table_schema": schema_name,
                    "table_name": table,
                    "column_name": column,
                    "is_low_cardinality": is_low_cardinality,
                    "distinct_values": ', '.join([str(x) for x in distinct_values]) if is_low_cardinality else None
                }
            )

    result_json = json.dumps(low_cardinality_info, indent=4)

    return low_cardinality_info



def update_low_cardinality_column_tags(
    catalog_name: str, schema_name: str, tables: list, threshold: int = 10
):
    """
    This function identifies low cardinality columns in a list of tables and updates
    the 'is_low_cardinality' Tag.

    Parameters:
    - catalog_name (str): The catalog name.
    - schema_name (str): The schema name where the target tables are located.
    - tables (list): A list of table names to analyze for low cardinality columns.
    - threshold (int): The maximum number of distinct values that classify a column as 'low cardinality'. Default is 10.

    Returns:
    - None: The function performs an alter operation but returns no value.
    """

    # Use the identify_low_cardinality_columns function to get columns and cardinality information
    low_cardinality_info = identify_low_cardinality_columns(
        catalog_name, schema_name, tables, threshold
    )

    # Update tags
    for obj in low_cardinality_info:
        # update tag: has_low_cardinality
        update_tag_query = f"""
            ALTER TABLE
                {obj['table_catalog']}.{obj['table_schema']}.{obj['table_name']}
            ALTER COLUMN
                {obj['column_name']} {'SET' if obj['is_low_cardinality'] else 'UNSET'} 
                TAGS ('has_low_cardinality' {"= '"+obj['distinct_values']+"'" if obj['is_low_cardinality'] else ''});
            """
        spark.sql(update_tag_query)
        if obj['is_low_cardinality']:
            # update tag: has_check
            update_tag_query = f"""
                ALTER TABLE
                    {obj['table_catalog']}.{obj['table_schema']}.{obj['table_name']}
                ALTER COLUMN
                    {obj['column_name']} UNSET 
                    TAGS ('has_check');
                """
            spark.sql(update_tag_query)

            # update tag: has_comment
            update_tag_query = f"""
                ALTER TABLE
                    {obj['table_catalog']}.{obj['table_schema']}.{obj['table_name']}
                ALTER COLUMN
                    {obj['column_name']} UNSET 
                    TAGS ('has_comment');
                """
            spark.sql(update_tag_query)



def update_column_description(table_path: str, column_name: str, description: str):
    """
    This function updates the description (COMMENT) for a specified column in a table.

    Parameters:
    - table_path (str): The full path of the table, including the catalog and schema if applicable.
    - column_name (str): The name of the column to update the description for.
    - description (str): The description to be added as a comment on the column.

    Returns:
    - None: The function performs an ALTER operation but returns no value.
    """
    
    # SQL query to update comment
    update_comment_query = f"""
        ALTER TABLE {table_path}
        ALTER COLUMN {column_name}
        COMMENT "{description}";
    """
    # Execute
    spark.sql(update_comment_query)

    # SQL query to update tag: has_comment
    update_tag_query = f"""
        ALTER TABLE {table_path}
        ALTER COLUMN {column_name} 
        SET TAGS ('has_comment');
     """
    spark.sql(update_tag_query)





def retrieve_columns_metadata(catalog_name: str, schema_name: str, list_of_tables: list) -> dict:
    """
    Retrieves and processes column information for the specified tables in a given catalog and schema.

    Parameters:
    ----------
    catalog_name : str
        The name of the catalog to query (e.g., the database).
    schema_name : str
        The name of the schema within the catalog.
    list_of_tables : list
        A list of table names for which to query and process column information.

    Returns:
    -------
    dict
        A dictionary where the keys are fully qualified table names and the values are lists of dictionaries
        containing column metadata, including column name, nullable status, data type, comment, and any tags.
    """
    
    # Fetch columns of interest
    result = get_table_columns(catalog_name, schema_name, list_of_tables)
    
    # Group columns for each table that will be later used to generate either:
    # checks per table or enrich the column comments
    table_info = defaultdict(list)

    for table in list_of_tables:
        full_table_name = f"{catalog_name}.{schema_name}.{table}"
    
        table_columns = list(
            map(
                lambda obj: {
                    "column_name": obj["column_name"],
                    "is_nullable": obj["is_nullable"],
                    "data_type": obj["data_type"],
                    "comment": obj["comment"],
                    "tag_names": obj["tag_names"],
                    "tag_values": obj["tag_values"]
                },
                filter(lambda obj: obj["table_name"] == table, json.loads(result)),
            )
        )
        
        table_info[full_table_name] = table_columns
    
    return dict(table_info)



def update_column_checks(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    check: str,
    tag: str = None, # ai_generated if by LLM
    check_id: int = None,
) -> bool:
    """
    Upserts a data quality check record for a specified column in the `column_checks` table within the specified catalog.

    This function ensures the existence of the `column_checks` table in the given catalog and then performs an upsert operation for the specified
    column. If a record with matching identifiers (catalog, schema, table, column, and tag) already exists, it updates the check value.
    If no such record exists, it inserts a new record. If a `check_id` is provided, the upsert targets this specific record by `check_id`.

    Parameters:
        catalog_name (str): The name of the catalog containing the `column_checks` table.
        schema_name (str): The schema name of the target table.
        table_name (str): The name of the table for which the check is being recorded.
        column_name (str): The name of the column for which the check is being recorded.
        check (str): The data quality check to be applied.
        tag (str, optional): A tag associated with the check, default is `None` or "ai_generated" if created by an LLM.
        check_id (int, optional): The unique identifier for the check, used to specifically target an existing check for updates.

    Returns:
        bool: True if the operation is successful, False if an error occurs.
    """
    target_table = get_checks_target_table(catalog_name)

    # Define the schema explicitly to avoid data type inference issues
    schema = StructType(
        [
            StructField("check_id", IntegerType(), True),
            StructField("catalog_name", StringType(), False),
            StructField("schema_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("check", StringType(), False),
            StructField("tag", StringType(), True),
        ]
    )

    # Create a DataFrame with the new record using the explicit schema
    new_data = [
        (
            check_id,
            catalog_name,
            schema_name,
            table_name,
            column_name,
            check,
            tag,
        )
    ]

    new_df = spark.createDataFrame(new_data, schema=schema)

    # Create a temporary view for the new data
    new_df.createOrReplaceTempView("new_checks")
    
    # Perform the MERGE INTO operation

    merge_sql = (
        f"""
        MERGE INTO {target_table} AS target
        USING new_checks AS source
        ON target.catalog_name = source.catalog_name
          AND target.schema_name = source.schema_name
          AND target.table_name = source.table_name
          AND target.column_name = source.column_name
          AND target.tag = source.tag
        WHEN MATCHED THEN
          UPDATE SET target.check = source.check
        WHEN NOT MATCHED THEN
          INSERT (catalog_name, schema_name, table_name, column_name, check, tag)
          VALUES (source.catalog_name, source.schema_name, source.table_name, source.column_name, source.check, source.tag)
    """
        if check_id is None
        else f"""
        MERGE INTO {target_table} AS target
        USING new_checks AS source
        ON target.check_id = source.check_id
          AND target.schema_name = source.schema_name
          AND target.table_name = source.table_name
          AND target.column_name = source.column_name
        WHEN MATCHED THEN
          UPDATE SET target.check = source.check,
                      target.tag = source.tag
    """
    )
    if spark.table(f"{catalog_name}.data_quality.column_checks").count() == 0:
        new_df = new_df.drop("check_id")
        try:
            new_df.write.option("mergeSchema", "true").format("delta").mode(
                "append"
            ).saveAsTable(f"{catalog_name}.data_quality.column_checks")
            return True
        except Exception as e:
            print(f"Error while excecuting the write operation: {e}")
            return False
    else:
        try:
            spark.sql(merge_sql)
            return True
        except Exception as e:
            print(f"Error while excecuting the merge statement: {e}")
            return False



def add_column_check(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    check: str,
) -> bool:
    """
    Inserts a new data quality check record for a specified column into the `column_checks` table within the specified catalog.

    This function ensures the `column_checks` table in the given catalog is used, then inserts a new record with the specified check details.
    If the operation is successful, it returns `True`; otherwise, it returns `False` if an error occurs.

    Parameters:
        catalog_name (str): The name of the catalog containing the `column_checks` table.
        schema_name (str): The schema name of the target table.
        table_name (str): The name of the table for which the check is being recorded.
        column_name (str): The name of the column for which the check is being recorded.
        check (str): The data quality check to be applied.

    Returns:
        bool: True if the insertion is successful, False if an error occurs.
    """
    target_table = get_checks_target_table(catalog_name)

    # Define the schema explicitly to avoid data type inference issues
    schema = StructType(
        [
            StructField("catalog_name", StringType(), False),
            StructField("schema_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("check", StringType(), False),
        ]
    )

    # Create a DataFrame with the new record using the explicit schema
    new_data = [
        (
            catalog_name,
            schema_name,
            table_name,
            column_name,
            check,
        )
    ]

    new_df = spark.createDataFrame(new_data, schema=schema)

    # Insert the new record
    try:
        new_df.write.mode("append").saveAsTable(target_table)
        return True
    except Exception as e:
        print(f"Error while executing the insert operation: {e}")
        return False



def parse_column(column: str) -> Optional[str]:
    """
    Transforms a string representation of an column JSON containing tag names and values into
    the desired structure. Parses the string into a dictionary, then applies the transformation.
    
    Parameters:
    column : str
        The string representation of the input object (expected to be in JSON format).
    
    Returns:
    dict
        The transformed dictionary with a single tag_name and tag_value or None if not available.
    """
    try:
        data = json.loads(column)
    except json.JSONDecodeError:
        print("Invalid JSON string.")
        return None

    # Check if tag_names and tag_values are not empty
    if data.get('tag_names') and data.get('tag_values'):
        try:
            index = data['tag_names'].index('has_low_cardinality')
            tag_name = data['tag_names'][index]
            tag_value = data['tag_values'][index]
        except ValueError:
            tag_name = data['tag_names'][0] if data['tag_names'] else None
            tag_value = data['tag_values'][0] if data['tag_values'] else None
    else:
        tag_name = None
        tag_value = None

    # Return the transformed object
    transformed_data = {
        'column_name': data['column_name'],
        'is_nullable': data['is_nullable'],
        'data_type': data['data_type'],
        'comment': data['comment'],
        'tag_name': tag_name,
        'tag_value': tag_value
    }

    transformed_data = str(transformed_data)

    return transformed_data
