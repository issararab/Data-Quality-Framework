"""Exercises catalog.dq_config against a real local Delta table."""

import uuid

from lakescore.catalog.dq_config import get_configurations, update_table_dq_conf_parameters
from tests.conftest import TEST_CATALOG


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return f"schema_{suffix}", f"table_{suffix}"


def test_update_table_dq_conf_parameters_inserts_then_merges(spark):
    schema_name, table_name = _unique_names()

    inserted = update_table_dq_conf_parameters(
        spark,
        TEST_CATALOG,
        schema_name,
        table_name,
        low_cardinality_threshold=10,
        freshness_window="1d",
    )
    assert inserted is True

    config_table = f"{TEST_CATALOG}.data_quality.dq_config"
    config = get_configurations(spark, config_table)
    assert config[TEST_CATALOG][schema_name][table_name] == {
        "low_cardinality_threshold": 10,
        "freshness_window": "1d",
        "validity_window": "1d",
    }

    updated = update_table_dq_conf_parameters(
        spark,
        TEST_CATALOG,
        schema_name,
        table_name,
        low_cardinality_threshold=25,
        freshness_window="7d",
    )
    assert updated is True

    config = get_configurations(spark, config_table)
    row = config[TEST_CATALOG][schema_name][table_name]
    assert row["low_cardinality_threshold"] == 25
    assert row["freshness_window"] == "7d"
