"""Shared helpers for building the raw SQL strings LakeScore sends to Spark.

Spark SQL (as executed here via ``spark.sql(...)``) does not offer a
parameterized-query API for DDL/DML statements like ``ALTER TABLE ... COMMENT``
or ``ALTER TABLE ... SET TAGS`` on every Databricks Runtime this project may run
on, so string interpolation is unavoidable in some places. Any value that did not
originate from a trusted schema/catalog listing (LLM-generated text, raw column
data) must be escaped with :func:`escape_sql_string` before being spliced into a
statement.

Databricks does support parameterized queries (``spark.sql(query, args=...)``)
for a growing set of statement types. Evaluate migrating callers to that API
instead of manual escaping once the target Databricks Runtime's coverage for
``ALTER TABLE`` statements is confirmed.
"""

from __future__ import annotations


def escape_sql_string(value: str) -> str:
    """Escapes backslashes and quote characters so ``value`` is safe to splice into a
    single- or double-quoted Spark SQL string literal.

    Parameters:
        value (str): The raw string to escape (e.g. an LLM-generated description or a
            raw column data value).

    Returns:
        str: The escaped string. Backslashes are escaped first so the escaping of quote
            characters isn't itself re-escaped.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
