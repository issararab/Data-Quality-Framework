from datetime import timedelta

import pytest

from lakescore.quality.freshness import extract_time_components, parse_threshold


@pytest.mark.parametrize(
    "threshold_str,expected",
    [
        ("3d", timedelta(days=3)),
        ("1h", timedelta(hours=1)),
        ("30m", timedelta(minutes=30)),
        ("1d6h", timedelta(days=1, hours=6)),
        ("1h30m", timedelta(hours=1, minutes=30)),
    ],
)
def test_parse_threshold(threshold_str, expected):
    assert parse_threshold(threshold_str) == expected


def test_parse_threshold_rejects_invalid_format():
    with pytest.raises(ValueError):
        parse_threshold("not-a-duration")


def test_extract_time_components():
    delta = timedelta(days=2, hours=3, minutes=45)
    assert extract_time_components(delta) == (2, 3, 45)
