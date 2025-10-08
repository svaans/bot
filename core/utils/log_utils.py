"""Utilities para sanitizar extras de logging y emisión segura de campos."""
from __future__ import annotations

import logging
from typing import Any, Mapping, MutableMapping

__all__ = ["safe_extra", "log_kv"]


# Construimos el set de atributos reservados de ``LogRecord`` en runtime.
_RESERVED = set(logging.LogRecord(None, None, "", 0, "", (), None).__dict__.keys())


def safe_extra(extra: Mapping[str, Any] | MutableMapping[str, Any] | None) -> dict[str, Any]:
    """Devuelve un ``dict`` seguro para usar como ``extra`` en logging.

    Se renombran automáticamente las claves que colisionan con atributos reservados
    del :class:`logging.LogRecord` agregando un guion bajo al final.
    """

    if not extra:
        return {}

    safe: dict[str, Any] = {}
    for key, value in extra.items():
        if key in _RESERVED:
            safe[f"{key}_"] = value
        else:
            safe[key] = value
    return safe


def log_kv(log: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    """Registra un mensaje garantizando ``extra`` sin colisiones."""

    log.log(level, msg, extra=safe_extra(fields))
