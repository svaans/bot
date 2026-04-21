"""Utilities de logging estructurado en JSON para el bot.

Nivel de salida por consola: variable de entorno ``BOT_LOG_LEVEL`` (``DEBUG``,
``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``). Por defecto ``INFO``: operación
normal sin inundar la terminal. Usa ``BOT_LOG_LEVEL=DEBUG`` para diagnóstico
detallado (p. ej. trazas de vela y datafeed).
"""
from __future__ import annotations

import io
import json
import logging
import math
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
    "console_log_level",
    "log_decision",
    "_should_log",
]

_CONSOLE_LEVEL_ALIASES: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def console_log_level() -> int:
    """Nivel mínimo de severidad para la consola JSON (env ``BOT_LOG_LEVEL``)."""

    raw = (os.getenv("BOT_LOG_LEVEL") or "INFO").strip().upper()
    return _CONSOLE_LEVEL_ALIASES.get(raw, logging.INFO)


_LOCK = threading.Lock()
_CONFIGURED: dict[str, logging.Logger] = {}
_LAST_EVENTS: dict[str, float] = {}
_RESERVED_ATTRS = set(logging.makeLogRecord({}).__dict__.keys())


def _json_safe(value: Any, *, _depth: int = 0, _max_depth: int = 5) -> Any:
    """Transforma ``value`` en un objeto serializable en JSON."""

    if _depth >= _max_depth:
        return repr(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if value is None or isinstance(value, (str, int, bool)):
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
        extras_raw = {k: v for k, v in record.__dict__.items() if k not in _RESERVED_ATTRS}
        # ``extra={"timestamp": ms_vela}`` termina en ``record.__dict__`` y, al hacer
        # ``data.update``, pisa la hora ISO del evento. El ms de vela va como ``candle_ts``.
        if "timestamp" in extras_raw:
            extras_raw = dict(extras_raw)
            extras_raw["candle_ts"] = extras_raw.pop("timestamp")
        extras = {k: _json_safe(v) for k, v in extras_raw.items()}
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
    handler.setLevel(console_log_level())
    return handler


def _ensure_root_logger() -> None:
    """Asegura un ``StreamHandler`` JSON en el root y un nivel de consola coherente."""

    level = console_log_level()
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.addHandler(_build_handler())
    else:
        # Reutilizamos el primer handler existente asegurando que emita a stdout.
        has_stream_handler = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
        if not has_stream_handler:
            root_logger.addHandler(_build_handler())
        else:
            for h in root_logger.handlers:
                if isinstance(h, logging.StreamHandler) and isinstance(
                    getattr(h, "formatter", None), _JsonFormatter
                ):
                    h.setLevel(level)
    root_logger.setLevel(level)


_ensure_root_logger()


def _set_minimum_level(logger_name: str, min_level: int) -> None:
    """Eleva ``logger_name`` hasta ``min_level`` si hoy permite DEBUG en exceso."""

    logger = logging.getLogger(logger_name)
    current_level = logger.level
    if current_level in (logging.NOTSET, 0) or current_level < min_level:
        logger.setLevel(min_level)


# ---------------------------------------------------------------------------
# BLOQUE DE DIAGNÓSTICO TEMPORAL — REVERTIR CUANDO TERMINE LA DEPURACIÓN.
#
# Durante la fase actual de auditoría del flujo de ejecución del bot queremos
# ver en consola sin tener que exportar variables de entorno desde la shell.
# Los sets siguientes se SUMAN a lo que venga por env var (``DEBUG_LOGGERS`` /
# ``UNSILENCE_LOGGERS``). Para volver al comportamiento normal basta con
# vaciar estos sets (o borrarlos) y dejar que el operador controle el detalle
# por entorno.
# ---------------------------------------------------------------------------
# Nota: no incluir ``binance_api.websocket`` aquí: ``_configure_noisy_loggers``
# debe poder elevarlo a INFO (tests + menos ruido). Para DEBUG puntual usar
# ``DEBUG_LOGGERS=binance_api.websocket`` o ``BOT_LOG_LEVEL=DEBUG``.
_DEBUG_DIAG_DEFAULT: set[str] = {
    "datafeed",
    "procesar_vela",
}
_UNSILENCE_DIAG_DEFAULT: set[str] = {
    "orders",
    "engine",
    "entry_verifier",
    "risk",
    "capital_manager",
    "entradas",
    "filtro_entradas",
}
# ---------------------------------------------------------------------------


def _debug_loggers_override() -> set[str]:
    """Loggers que el operador quiere forzar a DEBUG (env ``DEBUG_LOGGERS``).

    Útil para depurar puntualmente (p. ej. ``DEBUG_LOGGERS=binance_api.websocket,datafeed``)
    sin tener que bajar todo el runtime con ``BOT_LOG_LEVEL=DEBUG`` (que inunda
    la consola con ruido de terceros). También se fusiona con
    ``_DEBUG_DIAG_DEFAULT`` mientras dure el bloque de diagnóstico temporal.
    """

    raw = os.getenv("DEBUG_LOGGERS", "") or ""
    env_set = {item.strip() for item in raw.split(",") if item.strip()}
    return env_set | _DEBUG_DIAG_DEFAULT


def _configure_noisy_loggers() -> None:
    """Ajusta loggers ruidosos de terceros para no quedar por debajo de la consola."""

    floor = max(logging.INFO, console_log_level())
    noisy_loggers: dict[str, int] = {
        "websockets.client": floor,
        "websockets.protocol": floor,
        "binance_api.websocket": floor,
    }
    overrides = _debug_loggers_override()
    for name, min_level in noisy_loggers.items():
        if name in overrides:
            logging.getLogger(name).setLevel(logging.DEBUG)
            continue
        _set_minimum_level(name, min_level)

    # Loggers explícitamente pedidos en DEBUG aunque no estén en la lista de ruidosos.
    for name in overrides:
        if name in noisy_loggers:
            continue
        logging.getLogger(name).setLevel(logging.DEBUG)


_configure_noisy_loggers()


def _unsilence_loggers_override() -> set[str]:
    """Loggers para los que se ignora ``modo_silencioso=True`` (env ``UNSILENCE_LOGGERS``).

    Permite reactivar a INFO loggers de componentes que por defecto están
    silenciados (``orders``, ``engine``, ``entry_verifier``, …) sin tocar los
    archivos donde se crean. Uso típico para diagnóstico:

        UNSILENCE_LOGGERS=orders,engine,entry_verifier python main.py

    Mientras dure el bloque de diagnóstico temporal también se fusiona con
    ``_UNSILENCE_DIAG_DEFAULT`` para no depender de variables de entorno.
    """

    raw = os.getenv("UNSILENCE_LOGGERS", "") or ""
    env_set = {item.strip() for item in raw.split(",") if item.strip()}
    return env_set | _UNSILENCE_DIAG_DEFAULT


def configurar_logger(nombre: str, *, modo_silencioso: bool | None = None, nivel: int | None = None) -> logging.Logger:
    """Devuelve un logger configurado con formato JSON y singleton por nombre."""
    base = console_log_level()
    with _LOCK:
        if nombre in _CONFIGURED:
            logger = _CONFIGURED[nombre]
        else:
            logger = logging.getLogger(nombre)
            logger.setLevel(base)
            logger.handlers.clear()
            logger.addHandler(_build_handler())
            logger.propagate = False
            _CONFIGURED[nombre] = logger
        if nombre in {"trader", "trader_modular"}:
            # Estos loggers deben propagar al root para asegurar visibilidad completa.
            logger.handlers.clear()
            logger.setLevel(base)
            logger.propagate = True
    if nivel is not None:
        logger.setLevel(nivel)
    if modo_silencioso is None:
        modo_silencioso = os.getenv("BOT_SILENT_LOGGERS", "").lower() in {"1", "true", "yes"}
    # Escape de diagnóstico: si el operador pide un logger en ``UNSILENCE_LOGGERS``
    # ignoramos cualquier ``modo_silencioso=True`` hardcoded en el módulo cliente.
    if modo_silencioso and nombre in _unsilence_loggers_override():
        modo_silencioso = False
    if modo_silencioso:
        logger.setLevel(max(logger.level, logging.WARNING))
    # Override DEBUG aplicado DESPUÉS del resto: si el operador pidió este
    # logger en ``DEBUG_LOGGERS`` (o está en ``_DEBUG_DIAG_DEFAULT``) debe
    # quedar en DEBUG independientemente de lo que haga el módulo cliente.
    # Sin esto, cada ``configurar_logger(...)`` recién importado resetea al
    # nivel base (``BOT_LOG_LEVEL``) y el override inicial se pierde.
    if nombre in _debug_loggers_override():
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            try:
                handler.setLevel(logging.DEBUG)
            except Exception:
                continue
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
