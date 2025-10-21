"""Tests para garantizar que el logger JSON emite cargas válidas."""
from __future__ import annotations

import io
import json
import logging

from core.utils import logger as logger_module


def test_json_formatter_normalizes_non_finite_numbers() -> None:
    """Los valores no finitos deben serializarse como ``null`` para Prometheus."""

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logger_module._JsonFormatter())  # type: ignore[attr-defined]

    logger_name = "test.json_formatter.non_finite"
    test_logger = logging.getLogger(logger_name)
    previous_handlers = list(test_logger.handlers)
    previous_level = test_logger.level
    propagate_state = test_logger.propagate

    try:
        test_logger.handlers = [handler]
        test_logger.setLevel(logging.DEBUG)
        test_logger.propagate = False

        test_logger.info(
            "metrics snapshot",
            extra={
                "positive": float("inf"),
                "negative": float("-inf"),
                "nan": float("nan"),
            },
        )

        handler.flush()
        stream.seek(0)
        payload = json.loads(stream.getvalue())

        assert payload["positive"] is None
        assert payload["negative"] is None
        assert payload["nan"] is None
    finally:
        test_logger.handlers = previous_handlers
        test_logger.setLevel(previous_level)
        test_logger.propagate = propagate_state