"""Column tagging (`ALTER TABLE ... SET/UNSET TAGS`) is a Unity Catalog feature with no OSS
Spark equivalent, so it's tested here against a mocked `spark.sql`, asserting the exact
statement built, rather than executed for real."""

from unittest.mock import MagicMock

from lakescore.metadata.tags import set_column_tag, unset_column_tag


def test_set_column_tag_without_value():
    spark = MagicMock()

    result = set_column_tag(spark, "cat.schema.table", "col", "has_check")

    assert result is True
    (sql,), _ = spark.sql.call_args
    assert "ALTER TABLE cat.schema.table ALTER COLUMN col SET TAGS ('has_check')" in sql


def test_set_column_tag_with_value_is_escaped():
    spark = MagicMock()

    set_column_tag(spark, "cat.schema.table", "col", "has_low_cardinality", "O'Brien, Smith")

    (sql,), _ = spark.sql.call_args
    assert "'has_low_cardinality' = 'O\\'Brien, Smith'" in sql


def test_unset_column_tag():
    spark = MagicMock()

    result = unset_column_tag(spark, "cat.schema.table", "col", "has_comment")

    assert result is True
    (sql,), _ = spark.sql.call_args
    assert "ALTER TABLE cat.schema.table ALTER COLUMN col UNSET TAGS ('has_comment')" in sql


def test_set_column_tag_returns_false_on_failure():
    spark = MagicMock()
    spark.sql.side_effect = Exception("boom")

    assert set_column_tag(spark, "cat.schema.table", "col", "has_check") is False
