from databricks.sdk.runtime import *
import os
from soda.scan import Scan
os.environ['SODA_TELEMETRY_ENABLED'] = 'false'

def generate_soda_checks_yaml(catalog_name: str, schema_name: str, list_of_tables: list) -> dict:
    """
    Reads data from the demo.data_quality.column_checks table, filters by catalog_name,
    and generates a list of dictionaries where each dictionary contains the full table name
    and the Soda checks YAML for that table.
    
    :param catalog_name: The catalog_name to filter the data.
    :return: A list of dictionaries with 'table_name' and 'yaml_content' for each table.
    """
    # Read data from the demo.data_quality.column_checks table
    df = spark.table('demo.data_quality.column_checks')
    result_list = []
    
    # Iterate through the provided list of tables
    for table_name in list_of_tables:

        # Filter data for the specified catalog_name and table_name
        df_filtered = df.filter(
            (df.catalog_name == catalog_name) &
            (df.schema_name == schema_name) &
            (df.table_name == table_name))
        
        # Check if any data is returned for the table
        if df_filtered.count() == 0:
            print(f"No data found for {catalog_name}.{schema_name}.{table_name}")
            continue
        
        # Collect the data into a list of rows
        data = df_filtered.select('catalog_name', 'schema_name', 'table_name', 'check', 'tag', 'column_name').collect()
        
        # Convert the data into a list of dictionaries
        data_list = [row.asDict() for row in data]
        
        # Organize checks by table (use the full_table_name from the outer loop)
        checks_by_table = {}
        for row in data_list:
            full_table_name = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
            
            # Initialize the list of checks for the table if not already present
            if full_table_name not in checks_by_table:
                checks_by_table[full_table_name] = []
            
            # Clean up the check text
            check_clean = row['check'].strip()
            if check_clean.startswith('-'):
                check_clean = check_clean[1:].strip()
            
            checks_by_table[full_table_name].append(check_clean)
        
        # Generate a list of dictionaries containing the table name and YAML checks
        for full_table_name, checks in checks_by_table.items():
            yaml_lines = []
            yaml_lines.append(f"checks for {full_table_name}:")
            
            for check in checks:
                # Split multi-line checks
                check_lines = check.split('\n')
                # Add indentation for multi-line parameters
                formatted_check = '  - ' + check_lines[0]
                yaml_lines.append(formatted_check)
                if len(check_lines) > 1:
                    for line in check_lines[1:]:
                        yaml_lines.append('      ' + line.strip())
            
            # Join the lines to form the YAML content for the table
            yaml_content = '\n'.join(yaml_lines)
            # Append the result as a dictionary
            result_list.append({
                'table_name': full_table_name,
                'yaml_content': yaml_content
            })
    
    return result_list


def execute_soda_checks(catalog_name: str, schema_name: str, list_of_tables: list) -> dict:
    """
    Executes Soda checks for a list of tables within the specified catalog and schema, returning the results with table names.

    :param catalog_name: The catalog for which the Soda checks are generated.
    :param schema_name: The schema to which the tables belong.
    :param list_of_tables: List of table names for which Soda checks should be run.

    :return: A list of dictionaries containing the table names and their check summaries.
    """
    soda_check_results = []

    # Iterate through the provided list of tables
    for table_name in list_of_tables:
        print(f'Running tests for {catalog_name}.{schema_name}.{table_name}')
        # Generate Soda YAML for the specific catalog, schema, and table
        soda_checks = generate_soda_checks_yaml(catalog_name, schema_name, [table_name])
        yaml_content = soda_checks[0]['yaml_content']
        
        # Create and configure the Soda scan
        try:
            scan = Scan()
            scan.set_scan_definition_name("data_tests")  # Using a default scan definition name as it's not in the new function signature
            scan.set_data_source_name("spark_df")        # Using a default data source name
            scan.add_spark_session(spark)
            scan.add_sodacl_yaml_str(yaml_content)
            scan.execute()

            # Get the scan results
            scan_results = scan.get_scan_results()

            # Parse the scan results and include the table name
            check_results = _parse_scan_results(table_name, schema_name, table_name, scan_results)

            # Append the results for this table to the final list
            soda_check_results.append(check_results)
        except Exception as e:
            print(f'Error running Soda checks for {catalog_name}.{schema_name}.{table_name}: {str(e)}')

    return soda_check_results

def _parse_scan_results(catalog_name: str, schema_name: str, table_name: str, scan_result: dict) -> dict:
    """
    Parses the scan result for a given table and returns a dictionary with the full table name,
    check summary, and a boolean indicating if all checks have passed.
    
    :param catalog_name: The catalog name of the table.
    :param schema_name: The schema name of the table.
    :param table_name: The name of the table for which the scan was performed.
    :param scan_result: The dictionary containing the scan result.

    :return: A dictionary containing the full table name and a list of check summaries.
    """
    # Concatenate catalog, schema, and table names to form the full table name
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    check_summary = []
    all_checks_passed = True

    # Iterate over the checks in the scan result
    for check in scan_result['checks']:
        is_passed = check['outcome'] == 'pass'
        check_info = {
            'check_name': check['name'],
            'is_passed': is_passed,
            'check_definition': check['definition'],
            'failed_value_count': check['diagnostics']['value']
        }

        check_summary.append(check_info)
        # If any check fails, set all_checks_passed to False
        if not is_passed:
            all_checks_passed = False

    return {
        'table_name': full_table_name,
        'all_checks_passed': all_checks_passed,
        'check_summary': check_summary
    }
