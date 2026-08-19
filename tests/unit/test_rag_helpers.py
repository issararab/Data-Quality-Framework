from lakescore.generators._rag_helpers import (
    extract_comment_for_retriever,
    extract_user_query_string,
    retrieve_and_format_docs,
)


def test_extract_user_query_string_returns_latest_message_content():
    messages = [{"role": "user", "content": "first"}, {"role": "user", "content": "second"}]
    assert extract_user_query_string(messages) == "second"


def test_extract_comment_for_retriever_reads_comment_field():
    column = {"column_name": "email", "comment": "customer email address"}
    messages = [{"role": "user", "content": column}]
    assert extract_comment_for_retriever(messages) == "customer email address"


class _FakeIndex:
    """Stands in for `databricks.vector_search.client.VectorSearchIndex` in tests."""

    def __init__(self, rows: list[list[object]]) -> None:
        self._rows = rows
        self.last_call: dict[str, object] | None = None

    def similarity_search(
        self, query_text: str, columns: list[str], num_results: int
    ) -> dict[str, object]:
        self.last_call = {"query_text": query_text, "columns": columns, "num_results": num_results}
        return {"result": {"data_array": self._rows}}


def test_retrieve_and_format_docs_reshapes_results():
    index = _FakeIndex(
        rows=[["a SodaCL description", "check-1"], ["another description", "check-2"]]
    )

    docs = retrieve_and_format_docs(index, "customer email address", k=2)

    assert docs == [
        {"page_content": "a SodaCL description", "metadata": {"check_id": "check-1"}},
        {"page_content": "another description", "metadata": {"check_id": "check-2"}},
    ]
    assert index.last_call == {
        "query_text": "customer email address",
        "columns": ["description", "check_id"],
        "num_results": 2,
    }


def test_retrieve_and_format_docs_handles_empty_results():
    index = _FakeIndex(rows=[])
    assert retrieve_and_format_docs(index, "no matches") == []
