"""Utilities de logging estructurado en JSON para el bot."""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, MutableMapping, Optional

__all__ = [
    "configurar_logger",
    "log_decision",
    "_should_log",
]


_LOCK = threading.Lock()
_CONFIGURED: dict[str, logging.Logger] = {}
_LAST_EVENTS: dict[str, float] = {}
_RESERVED_ATTRS = set(logging.makeLogRecord({}).__dict__.keys())


class _JsonFormatter(logging.Formatter):
    """Formatter que serializa cada registro como JSON compacto."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - trivial
        data: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack"] = self.formatStack(record.stack_info)
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED_ATTRS}
        if extras:
            data.update(extras)
        return json.dumps(data, ensure_ascii=False)


def _build_handler() -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    formatter = _JsonFormatter()
    handler.setFormatter(formatter)
    return handler


def configurar_logger(nombre: str, *, modo_silencioso: bool | None = None, nivel: int | None = None) -> logging.Logger:
    """Devuelve un logger configurado con formato JSON y singleton por nombre."""
    with _LOCK:
        if nombre in _CONFIGURED:
            logger = _CONFIGURED[nombre]
        else:
            logger = logging.getLogger(nombre)
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
            logger.handlers.clear()
            logger.addHandler(_build_handler())
            _CONFIGURED[nombre] = logger
    if nivel is not None:
        logger.setLevel(nivel)
    if modo_silencioso is None:
        modo_silencioso = os.getenv("BOT_SILENT_LOGGERS", "").lower() in {"1", "true", "yes"}
    if modo_silencioso:
        logger.setLevel(max(logger.level, logging.WARNING))
    return logger


def _should_log(key: str, *, every: float = 5.0) -> bool:
    """Rate limiting simple para logs ruidosos."""
    ahora = time.monotonic()
    ultimo = _LAST_EVENTS.get(key)
    if ultimo is None or (ahora - ultimo) >= every:
        _LAST_EVENTS[key] = ahora
        return True
    return False


def log_decision(
    logger: logging.Logger,
    accion: str,
    operation_id: str | int | None,
    entrada: MutableMapping[str, Any] | Dict[str, Any] | None,
    validaciones: MutableMapping[str, Any] | Dict[str, Any] | None,
    resultado: str,
    detalle: Optional[Dict[str, Any]] = None,
    *,
    nivel: int = logging.INFO,
) -> None:
    """Registra una decisión operativa de forma estructurada."""
    payload = {
        "event": "decision",
        "accion": accion,
        "operation_id": operation_id,
        "entrada": entrada or {},
        "validaciones": validaciones or {},
        "resultado": resultado,
        "detalle": detalle or {},
    }
    logger.log(nivel, f"decision:{accion}", extra={"decision": payload})