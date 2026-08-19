import ast
import json

from lakescore.metadata.columns import parse_column


def _column_json(**overrides) -> str:
    base = {
        "column_name": "status",
        "is_nullable": "YES",
        "data_type": "string",
        "comment": "order status",
        "tag_names": [],
        "tag_values": [],
    }
    base.update(overrides)
    return json.dumps(base)


def test_parse_column_prefers_low_cardinality_tag():
    column = _column_json(
        tag_names=["has_comment", "has_low_cardinality"], tag_values=["", "OPEN, CLOSED"]
    )

    result = ast.literal_eval(parse_column(column))

    assert result["tag_name"] == "has_low_cardinality"
    assert result["tag_value"] == "OPEN, CLOSED"


def test_parse_column_falls_back_to_first_tag_when_no_low_cardinality_tag():
    column = _column_json(tag_names=["has_comment"], tag_values=["set"])

    result = ast.literal_eval(parse_column(column))

    assert result["tag_name"] == "has_comment"
    assert result["tag_value"] == "set"


def test_parse_column_returns_none_tags_when_no_tags_present():
    result = ast.literal_eval(parse_column(_column_json()))
    assert result["tag_name"] is None
    assert result["tag_value"] is None


def test_parse_column_returns_none_for_invalid_json():
    assert parse_column("not json") is None
