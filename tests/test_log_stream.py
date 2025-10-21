from __future__ import annotations

import io

import pytest

from tools import log_stream


def test_iter_records_filters_out_invalid_lines_and_merges_payload() -> None:
    raw = io.StringIO(
        """
\x1b[32m{"level": "INFO", "message": "{\\"foo\\": 1}"}\x1b[0m
not json at all
{"level": "DEBUG", "message": "hello"}
{"level": "INFO", "message": 123}
{"level": "INFO", "message": "{\\"foo\\": 2, \\"bar\\": true}"}
"""
    )
    records = list(log_stream.iter_records(raw))
    assert records[0]["foo"] == 1
    assert records[1]["message"] == "hello"
    assert "foo" not in records[2]
    assert records[3]["foo"] == 2
    assert records[3]["bar"] is True


@pytest.mark.parametrize(
    "loggers,levels,expected",
    [
        (None, None, 2),
        (["core.metrics"], None, 1),
        (None, ["error"], 1),
        (["core.metrics"], ["ERROR"], 0),
        (["unknown"], None, 0),
    ],
)
def test_process_stream_applies_filters(loggers, levels, expected) -> None:
    raw = io.StringIO(
        """
{"logger": "core.metrics", "level": "INFO", "message": "{}"}
{"logger": "hot_reload", "level": "ERROR", "message": "{}"}
"""
    )
    records = log_stream.process_stream(raw, loggers=loggers, levels=levels)
    assert len(records) == expected


def test_non_finite_values_are_normalized() -> None:
    raw = io.StringIO('{"value": NaN, "other": Infinity, "neg": -Infinity}\n')
    records = list(log_stream.iter_records(raw))
    assert len(records) == 1
    record = records[0]
    assert record["value"] is None
    assert record["other"] is None
    assert record["neg"] is None