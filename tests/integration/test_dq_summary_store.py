"""Exercises catalog.dq_summary against a real local Delta table.

`test_upsert_rejects_malformed_metric_keys` is a direct regression test for the bug fixed
earlier in this project's history: metric dicts with trailing-space keys (e.g.
`"has_a_valid_owner "`) produced invalid SQL column references and failed at merge time. Real
SQL execution (not a mock) is what catches this class of bug.
"""

import uuid

import pytest
from pyspark.errors import PySparkException

from lakescore.catalog.dq_summary import get_dq_summary_target_table, upsert_table_dq_summary
from tests.conftest import TEST_CATALOG


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return f"schema_{suffix}", f"table_{suffix}"


def test_upsert_inserts_then_updates_only_given_metrics(spark):
    schema_name, table_name = _unique_names()

    upsert_table_dq_summary(
        spark, TEST_CATALOG, schema_name, table_name, {"is_fresh": True, "columns_valid": False}
    )

    dq_summary = get_dq_summary_target_table(spark, TEST_CATALOG)
    row = spark.table(dq_summary).filter(f"table_name = '{table_name}'").collect()[0]
    assert row["is_fresh"] is True
    assert row["columns_valid"] is False
    assert row["has_a_valid_owner"] is False  # untouched column keeps its schema default

    upsert_table_dq_summary(
        spark, TEST_CATALOG, schema_name, table_name, {"has_a_valid_owner": True}
    )

    row = spark.table(dq_summary).filter(f"table_name = '{table_name}'").collect()[0]
    assert row["has_a_valid_owner"] is True
    assert row["is_fresh"] is True  # earlier upsert's value is preserved, not overwritten


def test_upsert_rejects_malformed_metric_keys(spark):
    schema_name, table_name = _unique_names()

    with pytest.raises(PySparkException):
        upsert_table_dq_summary(spark, TEST_CATALOG, schema_name, table_name, {"is_fresh ": True})
