"""Utilities para sanitizar extras de logging y emisión segura de campos."""
from __future__ import annotations

import logging
from typing import Any, Mapping, MutableMapping

__all__ = ["safe_extra", "log_kv", "truncate_for_log", "summarize_telegram_api_result"]


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


def truncate_for_log(value: Any, max_chars: int = 256) -> str:
    """Acorta texto arbitrario para ``extra`` de logs (evita volcados enormes)."""

    text = "" if value is None else str(value)
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 3] + "..."


def summarize_telegram_api_result(data: Any) -> dict[str, Any]:
    """Versión segura para métricas/eventos: sin texto completo del mensaje enviado."""

    if not isinstance(data, dict):
        return {"ok": False, "note": "non_object_response"}
    out: dict[str, Any] = {"ok": bool(data.get("ok"))}
    desc = data.get("description")
    if desc is not None:
        out["description"] = truncate_for_log(str(desc), 400)
    err = data.get("error_code")
    if err is not None:
        out["error_code"] = err
    result = data.get("result")
    if isinstance(result, dict):
        mid = result.get("message_id")
        if mid is not None:
            out["message_id"] = mid
    return out
