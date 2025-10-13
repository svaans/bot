from __future__ import annotations

"""Funciones auxiliares para la gestión de eventos del ``DataFeed``."""

from typing import Any

from core.metrics import WS_CONNECTED_GAUGE

from ._shared import log


def set_ws_connection_metric(value: float) -> None:
    """Actualiza la métrica de conexión del WebSocket, ignorando fallos."""

    try:
        WS_CONNECTED_GAUGE.set(value)
    except Exception:
        # La métrica puede no estar inicializada en entornos de prueba.
        pass


def emit_bus_signal(feed: "DataFeed", event: str, payload: dict[str, Any]) -> None:
    """Emite señales hacia un ``event_bus`` opcional."""

    bus = getattr(feed, "event_bus", None)
    if bus is None:
        return
    emit = getattr(bus, "emit", None)
    if callable(emit):
        try:
            emit(event, payload)
        except Exception:
            log.debug("No se pudo emitir evento %s en event_bus", event, exc_info=True)


def mark_ws_state(feed: "DataFeed", connected: bool, payload: dict[str, Any] | None = None) -> None:
    """Registra el estado del WS en métricas y bus de eventos."""

    base: dict[str, Any] = {"intervalo": feed.intervalo}
    if payload:
        base.update(payload)
    set_ws_connection_metric(1.0 if connected else 0.0)
    event = "ws_connected" if connected else "ws_disconnected"
    emit_bus_signal(feed, event, base)
    if not connected:
        log.info("ws_connect:closed", extra={**base, "stage": "DataFeed"})


def emit_event(feed: "DataFeed", event: str, data: dict[str, Any]) -> None:
    """Invoca el callback ``on_event`` del feed si está definido."""

    if feed.on_event:
        try:
            feed.on_event(event, data)
        except Exception:
            log.exception("on_event lanzó excepción")


def signal_ws_connected(feed: "DataFeed", symbol: str | None = None) -> None:
    """Marca al feed como conectado al WS."""

    if feed.ws_connected_event.is_set():
        return
    feed.ws_connected_event.set()
    payload: dict[str, Any] = {"intervalo": feed.intervalo}
    if symbol is not None:
        payload["symbol"] = symbol
    elif feed._symbols:
        payload["symbols"] = list(feed._symbols)
    log.info(
        "ws_connect:ok",
        extra={
            "intervalo": feed.intervalo,
            "stage": "DataFeed",
            "symbol": symbol,
            "symbols": payload.get("symbols"),
        },
    )
    mark_ws_state(feed, True, {k: v for k, v in payload.items() if k != "intervalo"})
    emit_event(feed, "ws_connect_ok", payload)


def signal_ws_failure(feed: "DataFeed", reason: Any) -> None:
    """Marca una falla de conexión y emite los eventos asociados."""

    feed._ws_failure_reason = reason if isinstance(reason, str) else repr(reason)
    if feed.ws_failed_event.is_set():
        return
    feed.ws_failed_event.set()
    payload = {"intervalo": feed.intervalo}
    if reason:
        payload["reason"] = reason
    mark_ws_state(feed, False, {k: v for k, v in payload.items() if k != "intervalo"})
    emit_event(feed, "ws_connect_failed", payload)


def _handle_limit_exceeded(
    feed: "DataFeed",
    *,
    key: str,
    reason: str,
    attempts: int,
    elapsed: float,
    limit_type: str,
) -> None:
    """Emite logs/eventos comunes cuando se excede algún límite de reconexión."""

    payload = {"key": key, "attempts": attempts, "elapsed": elapsed, "reason": reason}

    if limit_type == "attempts":
        payload["max_attempts"] = feed.max_reconnect_attempts
        log.error(
            "ws.max_retries_exceeded",
            extra={
                "key": key,
                "attempts": attempts,
                "max_attempts": feed.max_reconnect_attempts,
                "reason": reason,
            },
        )
        emit_event(feed, "ws_retries_exceeded", payload)
        signal_ws_failure(
            feed,
            f"Se superó el máximo de reintentos permitidos ({feed.max_reconnect_attempts})",
        )
        return

    payload["max_downtime"] = feed.max_reconnect_time
    log.error(
        "ws.downtime_exceeded",
        extra={
            "key": key,
            "elapsed": elapsed,
            "max_downtime": feed.max_reconnect_time,
            "reason": reason,
        },
    )
    emit_event(feed, "ws_downtime_exceeded", payload)
    signal_ws_failure(
        feed,
        f"Se superó el máximo de tiempo sin conexión ({feed.max_reconnect_time:.1f}s)",
    )


def _emit_retry_event(feed: "DataFeed", payload: dict[str, Any]) -> None:
    """Emite el evento ``ws_retry`` y evita duplicar lógica."""

    emit_event(feed, "ws_retry", payload)


def register_reconnect_attempt(feed: "DataFeed", key: str, reason: str) -> bool:
    """Valida límites de reconexión e informa métricas/eventos."""

    if feed.max_reconnect_attempts is None and feed.max_reconnect_time is None:
        _emit_retry_event(feed, {"key": key, "attempts": 1, "elapsed": 0.0, "reason": reason})
        return True

    import time

    now = time.monotonic()
    attempts = feed._reconnect_attempts.get(key, 0) + 1
    feed._reconnect_attempts[key] = attempts
    since = feed._reconnect_since.get(key)
    if since is None:
        since = now
        feed._reconnect_since[key] = since
    elapsed = max(0.0, now - since)

    payload = {"key": key, "attempts": attempts, "elapsed": elapsed, "reason": reason}

    if feed.max_reconnect_attempts is not None and attempts > feed.max_reconnect_attempts:
        _handle_limit_exceeded(
            feed,
            key=key,
            reason=reason,
            attempts=attempts,
            elapsed=elapsed,
            limit_type="attempts",
        )
        return False

    if feed.max_reconnect_time is not None and elapsed >= feed.max_reconnect_time:
        _handle_limit_exceeded(
            feed,
            key=key,
            reason=reason,
            attempts=attempts,
            elapsed=elapsed,
            limit_type="downtime",
        )
        return False

    _emit_retry_event(feed, payload)
    return True


def verify_reconnect_limits(feed: "DataFeed", key: str, reason: str) -> bool:
    """Comprueba los límites sin incrementar contadores para ``key``."""

    if feed.max_reconnect_attempts is None and feed.max_reconnect_time is None:
        return True

    if feed.ws_failed_event.is_set():
        return False

    import time

    attempts = feed._reconnect_attempts.get(key, 0)
    since = feed._reconnect_since.get(key)
    elapsed = 0.0
    if since is not None:
        elapsed = max(0.0, time.monotonic() - since)

    if feed.max_reconnect_attempts is not None and attempts > feed.max_reconnect_attempts:
        _handle_limit_exceeded(
            feed,
            key=key,
            reason=reason,
            attempts=attempts,
            elapsed=elapsed,
            limit_type="attempts",
        )
        return False

    if feed.max_reconnect_time is not None and since is not None and elapsed >= feed.max_reconnect_time:
        _handle_limit_exceeded(
            feed,
            key=key,
            reason=reason,
            attempts=attempts,
            elapsed=elapsed,
            limit_type="downtime",
        )
        return False

    return True


def reset_reconnect_tracking(feed: "DataFeed", key: str) -> None:
    """Limpia los contadores de reconexión para ``key``."""

    feed._reconnect_attempts.pop(key, None)
    feed._reconnect_since.pop(key, None)


def set_consumer_state(feed: "DataFeed", symbol: str, state: "ConsumerState") -> None:
    """Actualiza el estado interno de un consumidor y emite evento."""

    from ._shared import ConsumerState

    prev = feed._consumer_state.get(symbol)
    if prev == state:
        return
    feed._consumer_state[symbol] = state
    emit_event(feed, "consumer_state", {"symbol": symbol, "state": state.name.lower()})


# Importación tardía para evitar ciclos en tiempo de importación.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed
    from ._shared import ConsumerState
