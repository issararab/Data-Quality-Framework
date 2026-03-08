# Databricks notebook source
import ast
import os
import mlflow

from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain.schema.runnable import RunnableLambda
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks
from operator import itemgetter

# COMMAND ----------

LLM_MODEL_ENDPOINT_NAME = "ssbi-openai"#"databricks-meta-llama-3-1-70b-instruct"
VECTOR_SEARCH_ENDPOINT_NAME = "sodacl_indexer"
VECTOR_SEARCH_INDEX_PATH = "demo.data_quality.knowledge_base_index"
VECTOR_SEARCH_TABLE_PATH = "demo.data_quality.knowledge_base"

# COMMAND ----------

chain_config = {
    "llm_model_serving_endpoint_name": LLM_MODEL_ENDPOINT_NAME, 
    "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,
    "vector_search_index": VECTOR_SEARCH_INDEX_PATH,
    "vector_search_table": VECTOR_SEARCH_TABLE_PATH,
    "llm_prompt_template": """You are an assistant that generates YAML rules script for defining data quality checks and tests for described datasets using SodaCL (Soda Core Language).

- Inform yourself on the SodaCL correct syntax structure expected, based on the folowing examples:

<checks start>
checks for dim_customer:
  - missing_count(customer_id) = 0
  - invalid_count(house_owner_flag) = 0:
      valid values: [0, 1]
  - invalid_count(last_name) = 0:
      invalid regex: (?:XX)
</checks end>

+ First check breakdown:

dataset identifier:	dim_customer
check:	- missing_count(customer_id) = 0
metric:	missing_count
threshold:	0

+ Second check breakdown:

metric:	invalid_count
argument:	house_owner_flag
comparison symbol:	=
threshold:	0
configuration key:	valid values
configuration value(s):	0, 1

+ Third check breakdown:

metric:	invalid_count
argument:	last_name
comparison symbol or phrase:	=
threshold:	0
configuration key:	invalid regex
configuration value(s):	(?:XX)

Follow these steps to generate the checks:
    - For agiven column, you will get a dictionary with some metadata of the column.
    - The metadata include datatypes, some tags, and a comment describing the coulmn.
    - The comment will be used to match against some relevant checks to retrived from a knowledge base for this RAG app.
    - The context gives some syntax examples on special cases to help you create the YAML check lines.
    - Combined with your knowledge of SodaCL, use the following pieces retrieved context to generate the correct YAML lines.  
    - Some pieces of context may be irrelevant, in which case you should not use them to form the answer.
    - Generate 1, up to 2, most valid YAML check line for the given column.
    - The output should include no YAML descriptions and no additional information, only the check. 
    - The output should start always with "  -".
    
    
    \n\nContext: {context}""",
}

# COMMAND ----------

## Enable MLflow Tracing
mlflow.langchain.autolog()

## Load the chain's configuration
model_config = mlflow.models.ModelConfig(development_config=chain_config)

## Initialize the index configuration
vs_client = VectorSearchClient(disable_notice=True)
custom_index = vs_client.get_index(
    endpoint_name=model_config.get("vector_search_endpoint_name"),
    index_name=model_config.get("vector_search_index"),
)

# COMMAND ----------


def get_check_syntax_by_id(check_id: int, table_path: str) -> str | None:
    """
    Retrieves the `check_syntax` value from the specified table for the given check ID.

    Args:
        check_id (int): The ID of the check whose syntax needs to be retrieved.
        table_path (str): The path of the table where the check details are stored.

    Returns:
        str | None: The `check_syntax` value if found, otherwise `None`.
    """
    # Construct the SQL query dynamically
    query = f"""
    SELECT check_syntax
    FROM {table_path}
    WHERE check_id = {check_id}
    """

    # Execute the SQL query
    df = spark.sql(query)
    result = df.collect()

    if result:
        return result[0]["check_syntax"]
    else:
        return None

def format_context(docs: list[dict]) -> str:
    """
    Formats the retrieved documents into a prompt-friendly string format.

    Args:
        docs (list[dict]): A list of document dictionaries. Each dictionary should contain:
            - 'metadata' (dict): Contains metadata about the document, with the following key:
                - 'check_id' (str): A unique identifier for the document used to retrieve 
                  additional syntax details.
            - 'page_content' (str): The main text content of the document.

    Returns:
        str: A concatenated string where each document's 'check_syntax' and 
             'check_description' are formatted for inclusion in a language model prompt.
    """
    chunk_contents = [
        f"+check_syntax: \n {get_check_syntax_by_id(d['metadata']['check_id'], chain_config.get('vector_search_table'))}\ncheck_description: {d['page_content']}\n\n"
        for d in docs
    ]
    return "".join(chunk_contents)

def extract_user_query_string(chat_messages_array: list[dict]) -> str:
    """
    Extracts the content of the most recent message from the user.

    Args:
        chat_messages_array (list[dict]): A list of message dictionaries, where each dictionary
        contains details like role and content.

    Returns:
        str: The content of the most recent message from the user.
    """
    return chat_messages_array[-1]["content"]


def extract_comment_for_retriever(chat_messages_array: list[dict]) -> str:
    """
    Extracts the `comment` field from the content of the most recent user message.

    Args:
        chat_messages_array (list[dict]): A list of message dictionaries, where each dictionary
        contains details like role and content, and the content is itself a dictionary with a "comment" key.

    Returns:
        str: The `comment` field from the most recent user message content.
    """
    return chat_messages_array[-1]["content"]["comment"]

def retrieve_and_format_docs(query: str, k: int = 5) -> list[dict]:
    """
    Retrieves and formats documents from a similarity search on a specified index.

    Args:
        query (str): The search query used to retrieve relevant documents from the index.
        k (int, optional): The number of top results to retrieve. Defaults to 5.

    Returns:
        list[dict]: A list of dictionaries where each dictionary represents a document 
                    and includes:
            - 'page_content' (str): The main text content of the document.
            - 'metadata' (dict): Metadata about the document, containing:
                - 'check_id' (str): A unique identifier for each document.
    """
    # Perform the similarity search using the index
    results = custom_index.similarity_search(
        query_text=query,
        columns=["description", "check_id" ],
        num_results=k
    )
    # Extract documents from the search results
    docs = results.get('result', {}).get('data_array', [])
    docs = [{'page_content': d[0], 'metadata': {'check_id': d[1]}} for d in docs]
    return docs

# COMMAND ----------

# LLM model answering the final prompt
model = ChatDatabricks(
    endpoint=model_config.get("llm_model_serving_endpoint_name"),
    extra_params={"temperature": 0.01, "max_tokens": 500}
)

# RAG Chain
rag_generator = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "context": itemgetter("messages")
        | RunnableLambda(extract_comment_for_retriever)
        | RunnableLambda(retrieve_and_format_docs)
        | RunnableLambda(format_context),
    }
    | ChatPromptTemplate.from_messages([  ("system", model_config.get("llm_prompt_template")), ("user", "{question}")])
    | model
    | StrOutputParser()
)

# COMMAND ----------

mlflow.models.set_model(rag_generator)
