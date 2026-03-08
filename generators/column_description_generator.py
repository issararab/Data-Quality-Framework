# Databricks notebook source
import os
import mlflow

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks

# COMMAND ----------

model_config = {
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-1-70b-instruct",#"ssbi-openai"  
    "llm_prompt_template": """You are an assistant that generates a concise metadata description of a column in a given table. 
    - you will get a dictionary with some metadata of the column, including some tags and a comment. 
    - The comment represents the description we want to generate on the column based on all provided information. 
    - If the comment has a description, that would mean it is the existing description.
    - The comment might be enough and consisely describing the column.
    - Other metadata might change over time, and the comment/description too.
    - Use all the provided information to update the comment accordingly.
    - The comment / description should be concise and not more than 2 sentences.
    - Use the key word "valid values", in case of low cardinality columns to mention the list of possible values.
    - Output only the description nothing else.
    - Avoid any unnecessary single quotation marks or punctution in the output.
    - Output a clean description, not starting with a punctuation but ending with a full stop.
    
    \n\nContext: {context}""",
}

# COMMAND ----------


## Enable MLflow Tracing
mlflow.langchain.autolog()

## Load the model's configuration
model_config = mlflow.models.ModelConfig(development_config=model_config)

prompt = ChatPromptTemplate.from_messages(
    [  
        ("system", model_config.get("llm_prompt_template")), # Contains the instructions from the configuration
        ("user", "{question}") #user's query
    ]
)

# LLM model answering the final prompt
model = ChatDatabricks(
    endpoint=model_config.get("llm_model_serving_endpoint_name"),
    extra_params={"temperature": 0.01, "max_tokens": 250}
)


# COMMAND ----------

# model generator
generator = prompt | model | StrOutputParser()

# COMMAND ----------

mlflow.models.set_model(generator)

# COMMAND ----------


