# core/vela/order_open.py — reintentos y circuit breaker al crear órdenes
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Dict, Optional

from core.orders.order_open_status import OrderOpenStatus
from core.utils.log_utils import format_exception_for_log
from core.utils.logger import configurar_logger

from core.vela.circuit_breaker import (
    OrderCircuitBreakerOpen,
    OrderCircuitBreakerStore,
    _DEFAULT_ORDER_CIRCUIT_STORE,
    _ORDER_CIRCUIT_RESET_AFTER,
)

log = configurar_logger("procesar_vela")

_ORDER_CREATION_MAX_ATTEMPTS = 3
_ORDER_CREATION_INITIAL_BACKOFF = 0.5
_ORDER_CREATION_BACKOFF_FACTOR = 2.0
_ORDER_CREATION_BACKOFF_CAP = 5.0
_ORDER_CREATION_JITTER = 0.2


async def _abrir_orden(
    orders: Any,
    symbol: str,
    side: str,
    precio: float,
    sl: float,
    tp: float,
    meta: Dict[str, Any],
    *,
    trace_id: str | None = None,
    max_attempts: Optional[int] = None,
    circuit_store: OrderCircuitBreakerStore | None = None,
) -> OrderOpenStatus:
    """Wrapper robusto sobre OrderManager.crear(...) con reintentos solo ante excepciones.

    ``crear`` devuelve :class:`OrderOpenStatus`; un ``FAILED`` lógico (fondos,
    niveles, duplicado, etc.) no lanza excepción y **no** debe tratarse como
    apertura exitosa ni disparar notificaciones de éxito.
    """
    crear = getattr(orders, "crear", None)
    if not callable(crear):
        raise RuntimeError("OrderManager no implementa crear(...)")

    if precio <= 0:
        raise ValueError("precio_entrada inválido")
    if sl <= 0 or tp <= 0:
        log.debug("[%s] SL/TP no establecidos al abrir (sl=%.6f, tp=%.6f)", symbol, sl, tp)

    store = circuit_store or _DEFAULT_ORDER_CIRCUIT_STORE
    state = store.get_state(symbol, side)
    now = time.monotonic()
    state.reset_if_idle(now, _ORDER_CIRCUIT_RESET_AFTER)
    if state.opened_until and state.opened_until <= now:
        state.opened_until = 0.0
    if state.opened_until > now:
        raise OrderCircuitBreakerOpen(state.opened_until - now, state)

    attempts = max_attempts if max_attempts and max_attempts > 0 else _ORDER_CREATION_MAX_ATTEMPTS
    if attempts <= 0:
        attempts = 1

    base_meta: Dict[str, Any] = dict(meta or {})
    if trace_id:
        base_meta.setdefault("trace_id", trace_id)

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        payload_meta = dict(base_meta)
        try:
            res = crear(
                symbol=symbol,
                side=side,
                precio=precio,
                sl=sl,
                tp=tp,
                meta=payload_meta,
            )
            if asyncio.iscoroutine(res):
                res = await res
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exc = exc
            failure_now = time.monotonic()
            opened = state.record_failure(failure_now)
            log.warning(
                "orders_crear_retry",
                extra={
                    "symbol": symbol,
                    "side": side,
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "error": format_exception_for_log(exc),
                },
            )
            if opened:
                retry_after = max(0.0, state.opened_until - failure_now)
                raise OrderCircuitBreakerOpen(retry_after, state) from exc
            if attempt >= attempts:
                break

            delay = _ORDER_CREATION_INITIAL_BACKOFF * (_ORDER_CREATION_BACKOFF_FACTOR ** (attempt - 1))
            delay = min(delay, _ORDER_CREATION_BACKOFF_CAP)
            if _ORDER_CREATION_JITTER > 0:
                jitter = delay * _ORDER_CREATION_JITTER
                low = max(0.0, delay - jitter)
                high = delay + jitter
                delay = random.uniform(low, high)
            await asyncio.sleep(delay)
            continue

        if res in (OrderOpenStatus.OPENED, OrderOpenStatus.PENDING_REGISTRATION):
            state.record_success()
            return res

        log.warning(
            "orders_crear_rejected",
            extra={
                "symbol": symbol,
                "side": side,
                "status": getattr(res, "value", str(res)),
                "attempt": attempt,
            },
        )
        return OrderOpenStatus.FAILED

    if last_exc is not None:
        raise last_exc
    return OrderOpenStatus.FAILED
