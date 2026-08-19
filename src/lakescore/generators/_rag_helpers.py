"""Testable core of the SodaCL RAG chain: document formatting and retrieval, with every
external dependency (Spark, the Vector Search index) accepted as a parameter rather than
read from a module-level global.

`format_context`/`get_check_syntax_by_id` are testable against a local Spark session with a
`knowledge_base` table; they don't require a live Databricks Vector Search endpoint.
`extract_user_query_string`/`extract_comment_for_retriever` require no external dependency at
all. Only `retrieve_and_format_docs` needs a real (or fake/mock) Vector Search index client.
"""

from __future__ import annotations

from typing import Any, Protocol

from pyspark.sql import SparkSession


class SimilaritySearchIndex(Protocol):
    """The subset of `databricks.vector_search.client.VectorSearchIndex` this module depends
    on, so tests can pass a fake implementation instead of a live index."""

    def similarity_search(
        self, query_text: str, columns: list[str], num_results: int
    ) -> dict[str, Any]: ...


def get_check_syntax_by_id(spark: SparkSession, check_id: int, table_path: str) -> str | None:
    """Looks up `check_syntax` for `check_id` in the knowledge-base table.

    Returns:
        str | None: The syntax string, or `None` if no row matches.
    """
    result = spark.sql(
        f"SELECT check_syntax FROM {table_path} WHERE check_id = {check_id}"
    ).collect()
    return result[0]["check_syntax"] if result else None


def format_context(
    spark: SparkSession, knowledge_base_table: str, docs: list[dict[str, Any]]
) -> str:
    """Formats retrieved knowledge-base documents into the RAG prompt's `{context}` value.

    Parameters:
        spark (SparkSession): Active Spark session.
        knowledge_base_table (str): Fully qualified `knowledge_base` table path.
        docs (list[dict[str, Any]]): Retrieved documents, each with `page_content` and
            `metadata.check_id` (see `retrieve_and_format_docs`).

    Returns:
        str: Concatenated `check_syntax`/`check_description` blocks, one per document.
    """
    return "".join(
        f"+check_syntax: \n {get_check_syntax_by_id(spark, d['metadata']['check_id'], knowledge_base_table)}\n"
        f"check_description: {d['page_content']}\n\n"
        for d in docs
    )


def extract_user_query_string(chat_messages_array: list[dict[str, Any]]) -> str:
    """Returns the most recent chat message's `content`."""
    return chat_messages_array[-1]["content"]


def extract_comment_for_retriever(chat_messages_array: list[dict[str, Any]]) -> str:
    """Returns the `comment` field from the most recent chat message's `content` dict."""
    return chat_messages_array[-1]["content"]["comment"]


def retrieve_and_format_docs(
    index: SimilaritySearchIndex, query: str, k: int = 5
) -> list[dict[str, Any]]:
    """Runs a similarity search and reshapes the results into `{page_content, metadata}` dicts.

    Parameters:
        index (SimilaritySearchIndex): A Vector Search index (or test double) exposing
            `similarity_search`.
        query (str): The search query.
        k (int): Number of results to retrieve.

    Returns:
        list[dict[str, Any]]: `[{"page_content": description, "metadata": {"check_id": id}}, ...]`.
    """
    results = index.similarity_search(
        query_text=query, columns=["description", "check_id"], num_results=k
    )
    docs = results.get("result", {}).get("data_array", [])
    return [{"page_content": d[0], "metadata": {"check_id": d[1]}} for d in docs]
