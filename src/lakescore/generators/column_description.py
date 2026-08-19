"""Composition root for the column-description GenAI chain.

Logged/served via MLflow's "models from code" pattern (`mlflow.models.set_model`), which
requires this module to build and register its chain at import/exec time — so, unlike
`_rag_helpers.py`, it isn't unit-tested directly. Keep this file limited to wiring; put any new
logic worth testing in `_rag_helpers.py` or `prompts.py` instead.
"""

from __future__ import annotations

import os

import mlflow
from langchain_community.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable

from lakescore.generators.prompts import COLUMN_DESCRIPTION_SYSTEM_PROMPT

DEFAULT_LLM_MODEL_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


def build_chain(llm_model_endpoint: str | None = None) -> Runnable:
    """Assembles the column-description chain: prompt | model | output parser.

    Parameters:
        llm_model_endpoint (str | None): Databricks Model Serving endpoint name. Defaults to
            the `LAKESCORE_LLM_MODEL_ENDPOINT` env var, then `DEFAULT_LLM_MODEL_ENDPOINT`.
    """
    endpoint = llm_model_endpoint or os.environ.get(
        "LAKESCORE_LLM_MODEL_ENDPOINT", DEFAULT_LLM_MODEL_ENDPOINT
    )
    prompt = ChatPromptTemplate.from_messages(
        [("system", COLUMN_DESCRIPTION_SYSTEM_PROMPT), ("user", "{question}")]
    )
    model = ChatDatabricks(endpoint=endpoint, extra_params={"temperature": 0.01, "max_tokens": 250})
    return prompt | model | StrOutputParser()


mlflow.langchain.autolog()
generator = build_chain()
mlflow.models.set_model(generator)
