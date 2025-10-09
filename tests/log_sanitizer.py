from __future__ import annotations

from typing import Any, Dict, Iterable, List

from logging import LogRecord


_DEFAULT_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


def parse_records(records: Iterable[LogRecord]) -> List[Dict[str, Any]]:
    """Normaliza ``LogRecord`` en dicts comparables para los asserts."""

    parsed: List[Dict[str, Any]] = []
    for record in records:
        payload: Dict[str, Any] = {"logger": record.name, "message": record.getMessage()}
        for key, value in record.__dict__.items():
            if key in _DEFAULT_FIELDS:
                continue
            payload[key] = value
        parsed.append(payload)
    return parsed


def has_event(
    records: Iterable[LogRecord],
    logger: str,
    message_substr: str,
    **extras_like: Any,
) -> bool:
    """Verifica si existe un log que coincide con ``logger`` y ``message``."""

    for entry in parse_records(records):
        if entry.get("logger") != logger:
            continue
        if message_substr not in entry.get("message", ""):
            continue
        for key, expected in extras_like.items():
            if entry.get(key) != expected:
                break
        else:
            return True
    return False
