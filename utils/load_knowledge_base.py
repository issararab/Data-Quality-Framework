from databricks.sdk.runtime import *
import os


def create_knowledge_base_table_if_not_exists():
    """
    This function creates a Delta table named `knowledge_base` in the `demo.data_quality` schema if it does not already exist.

    After executing the query, it prints a message to confirm that the table was created or that it already exists.
    """

    create_table_query = """
    CREATE TABLE IF NOT EXISTS demo.data_quality.knowledge_base (
        check_id STRING,
        check_syntax STRING,
        description STRING
    )
    """
    spark.sql(create_table_query)
    print("Table created or already exists.")


def append_data_to_table():
    """
    This function appends data from a CSV file (`knowledge_base.csv`) to an existing Delta table named `knowledge_base` in the `demo.data_quality` schema.
    """
    # Get the root repository directory
    root_repo_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
    knowledge_base_path = os.path.join(root_repo_path, 'data', 'knowledge', 'knowledge_base.csv')

    knowledge_base_df = spark.read.format('csv').option('header', 'true').load(f'file:{knowledge_base_path}')

    # Append the data to the Delta table
    knowledge_base_df.write.format("delta").mode("append").saveAsTable("demo.data_quality.knowledge_base")
    print("knowledge base data appended to the table.")
