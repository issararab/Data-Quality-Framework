"""Exercises catalog.column_checks against a real local Delta table."""

import uuid

from lakescore.catalog.column_checks import (
    add_column_check,
    get_columns_with_checks,
    update_column_checks,
)
from tests.conftest import TEST_CATALOG


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return f"schema_{suffix}", f"table_{suffix}"


def test_update_column_checks_inserts_then_updates(spark):
    schema_name, table_name = _unique_names()

    inserted = update_column_checks(
        spark,
        TEST_CATALOG,
        schema_name,
        table_name,
        "email",
        "missing_count(email) = 0",
        tag="ai_generated",
    )
    assert inserted is True

    columns_with_checks = get_columns_with_checks(spark, TEST_CATALOG, schema_name, [table_name])
    full_table_name = f"{TEST_CATALOG}.{schema_name}.{table_name}"
    assert columns_with_checks[full_table_name] == ["email"]

    updated = update_column_checks(
        spark,
        TEST_CATALOG,
        schema_name,
        table_name,
        "email",
        "invalid_count(email) = 0",
        tag="ai_generated",
    )
    assert updated is True

    rows = (
        spark.table(f"{TEST_CATALOG}.data_quality.column_checks")
        .filter(f"table_name = '{table_name}' AND column_name = 'email'")
        .collect()
    )
    assert len(rows) == 1
    assert rows[0]["check"] == "invalid_count(email) = 0"


def test_add_column_check_inserts_without_upsert_matching(spark):
    schema_name, table_name = _unique_names()

    assert add_column_check(
        spark, TEST_CATALOG, schema_name, table_name, "status", "missing_count(status) = 0"
    )

    columns_with_checks = get_columns_with_checks(spark, TEST_CATALOG, schema_name, [table_name])
    assert "status" in columns_with_checks[f"{TEST_CATALOG}.{schema_name}.{table_name}"]
