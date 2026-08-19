"""Composition root for the RAG-grounded SodaCL check-generation chain.

Logged/served via MLflow's "models from code" pattern (`mlflow.models.set_model`), which
requires this module to build and register its chain at import/exec time — so, unlike
`_rag_helpers.py`, it isn't unit-tested directly. Keep this file limited to wiring; put any new
logic worth testing in `_rag_helpers.py` or `prompts.py` instead.

Resolves its catalog/endpoint configuration via `LakeScoreConfig.from_widgets`: reads the
`catalog_name` widget the calling notebook sets (see `notebooks/generate_checks.py`, which
declares it *before* importing this module so the widget exists by the time this file's
module-level code runs), falling back to `LAKESCORE_*` environment variables for any field
without a widget — which also covers standalone deployment to a Model Serving endpoint, where
there's no widget/notebook context at all and only env vars apply.
"""

from __future__ import annotations

from functools import partial
from operator import itemgetter

import mlflow
from databricks.sdk.runtime import dbutils, spark
from databricks.vector_search.client import VectorSearchClient
from langchain.schema.runnable import RunnableLambda
from langchain_community.chat_models import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable

from lakescore.config import LakeScoreConfig
from lakescore.generators._rag_helpers import (
    extract_comment_for_retriever,
    extract_user_query_string,
    format_context,
    retrieve_and_format_docs,
)
from lakescore.generators.prompts import COLUMN_CHECK_RAG_SYSTEM_PROMPT


def build_chain(config: LakeScoreConfig) -> Runnable:
    """Assembles the RAG chain: retrieve SodaCL context for the column's comment, then generate
    a check grounded in that context.

    Parameters:
        config (LakeScoreConfig): Supplies the vector-search endpoint/index, the knowledge-base
            table path, and the LLM endpoint.
    """
    vs_client = VectorSearchClient(disable_notice=True)
    index = vs_client.get_index(
        endpoint_name=config.vector_search_endpoint, index_name=config.vector_search_index
    )
    model = ChatDatabricks(
        endpoint=config.llm_model_endpoint, extra_params={"temperature": 0.01, "max_tokens": 500}
    )
    prompt = ChatPromptTemplate.from_messages(
        [("system", COLUMN_CHECK_RAG_SYSTEM_PROMPT), ("user", "{question}")]
    )

    return (
        {
            "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
            "context": itemgetter("messages")
            | RunnableLambda(extract_comment_for_retriever)
            | RunnableLambda(partial(retrieve_and_format_docs, index))
            | RunnableLambda(partial(format_context, spark, config.knowledge_base_table)),
        }
        | prompt
        | model
        | StrOutputParser()
    )


mlflow.langchain.autolog()
_config = LakeScoreConfig.from_widgets(dbutils)
rag_generator = build_chain(_config)
mlflow.models.set_model(rag_generator)
