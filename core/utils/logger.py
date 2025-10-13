"""Utilities de logging estructurado en JSON para el bot."""
from __future__ import annotations

import io
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal
from pathlib import Path
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


def _json_safe(value: Any, *, _depth: int = 0, _max_depth: int = 5) -> Any:
    """Transforma ``value`` en un objeto serializable en JSON."""

    if _depth >= _max_depth:
        return repr(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8", "replace")
    if is_dataclass(value):
        return _json_safe(asdict(value), _depth=_depth + 1, _max_depth=_max_depth)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(val, _depth=_depth + 1, _max_depth=_max_depth) for key, val in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _json_safe(item, _depth=_depth + 1, _max_depth=_max_depth)
            for item in value
        ]
    if isinstance(value, (set, frozenset)):
        return [
            _json_safe(item, _depth=_depth + 1, _max_depth=_max_depth)
            for item in value
        ]
    return repr(value)


def _flatten_args(args: Any) -> Any:
    """Normaliza los argumentos de logging para que sean serializables."""

    if isinstance(args, Mapping):
        return {str(key): _json_safe(val) for key, val in args.items()}
    if isinstance(args, tuple):
        return tuple(_json_safe(item) for item in args)
    if isinstance(args, list):
        return [_json_safe(item) for item in args]
    return _json_safe(args)


class _JsonFormatter(logging.Formatter):
    """Formatter que serializa cada registro como JSON compacto."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - trivial
        message = record.getMessage()
        if record.args:
            record.args = _flatten_args(record.args)
        data: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack"] = self.formatStack(record.stack_info)
        extras = {k: _json_safe(v) for k, v in record.__dict__.items() if k not in _RESERVED_ATTRS
        if extras:
            data.update(extras)
        return json.dumps(data, ensure_ascii=False, default=_json_safe)


class _SafeStreamHandler(logging.StreamHandler):
    """StreamHandler tolerante al cierre del *stream* durante la ejecución de tests."""

    _fallback_streams = ("__stdout__", "__stderr__")

    def __init__(self) -> None:
        super().__init__(self._resolve_stream())

    @staticmethod
    def _resolve_stream() -> io.TextIOBase:
        """Obtiene un *stream* estable priorizando ``sys.stdout`` real."""

        for candidate in ("stdout", *_SafeStreamHandler._fallback_streams):
            stream = getattr(sys, candidate, None)
            if isinstance(stream, io.TextIOBase) and not stream.closed:
                return stream
        return sys.__stdout__  # pragma: no cover - ruta de seguridad

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - comportamiento trivial
        stream = getattr(self, "stream", None)
        if not stream or getattr(stream, "closed", False):
            self.setStream(self._resolve_stream())
        try:
            super().emit(record)
        except ValueError:
            # Si el stream fue cerrado por el *test runner*, redirigimos al stdout real.
            self.setStream(self._resolve_stream())
            try:
                super().emit(record)
            except ValueError:
                # Como último recurso escribimos mediante el stdout real sin formateo.
                fallback = self._resolve_stream()
                msg = self.format(record)
                fallback.write(f"{msg}\n")
                fallback.flush()

    def setStream(self, stream: io.TextIOBase | None) -> None:  # pragma: no cover - simple
        current = getattr(self, "stream", None)
        if stream is current:
            return
        self.stream = stream
        if current and not getattr(current, "closed", False):
            try:
                current.flush()
            except ValueError:
                pass


def _build_handler() -> logging.Handler:
    handler = _SafeStreamHandler()
    formatter = _JsonFormatter()
    handler.setFormatter(formatter)
    return handler


def _ensure_root_logger() -> None:
    """Guarantee a StreamHandler in the root logger with DEBUG level."""

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.addHandler(_build_handler())
    else:
        # Reutilizamos el primer handler existente asegurando que emita a stdout.
        has_stream_handler = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
        if not has_stream_handler:
            root_logger.addHandler(_build_handler())
    root_logger.setLevel(logging.DEBUG)


_ensure_root_logger()


def configurar_logger(nombre: str, *, modo_silencioso: bool | None = None, nivel: int | None = None) -> logging.Logger:
    """Devuelve un logger configurado con formato JSON y singleton por nombre."""
    with _LOCK:
        if nombre in _CONFIGURED:
            logger = _CONFIGURED[nombre]
        else:
            logger = logging.getLogger(nombre)
            logger.setLevel(logging.DEBUG)
            logger.handlers.clear()
            logger.addHandler(_build_handler())
            logger.propagate = False
            _CONFIGURED[nombre] = logger
        if nombre in {"trader", "trader_modular"}:
            # Estos loggers deben propagar al root para asegurar visibilidad completa.
            logger.handlers.clear()
            logger.setLevel(logging.DEBUG)
            logger.propagate = True
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
