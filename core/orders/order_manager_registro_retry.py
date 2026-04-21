# core/orders/order_manager_registro_retry.py — reintentos tras fallo en registrar_orden
from __future__ import annotations

import asyncio
import random
import sqlite3
from typing import Any

from core.metrics import (
    limpiar_registro_pendiente,
    registrar_orden,
    registrar_orders_retry_scheduled,
    registrar_registro_error,
)
from core.orders import real_orders
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger("orders", modo_silencioso=True)

PERSISTENCE_ERRORS: tuple[type[BaseException], ...] = (OSError,)
if sqlite3 is not None:  # pragma: no cover - sqlite casi siempre disponible
    PERSISTENCE_ERRORS = PERSISTENCE_ERRORS + (sqlite3.Error,)

PERSISTENCE_ERROR_KEYWORDS = (
    "database",
    "sqlite",
    "disk",
    "io",
    "locked",
    "write",
)


def should_schedule_persistence_retry(manager: Any, exc: Exception | None) -> bool:
    if not manager._registro_retry_enabled or exc is None:
        return False
    if isinstance(exc, PERSISTENCE_ERRORS):
        return True
    message = str(exc).lower()
    return any(keyword in message for keyword in manager._registro_retry_error_keywords)


def schedule_registro_retry(manager: Any, symbol: str, *, reason: str | None = None) -> None:
    """Programa un reintento con backoff controlado tras fallas de persistencia."""

    if not manager._registro_retry_enabled:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    orden = manager.ordenes.get(symbol)
    if not orden or not getattr(orden, "registro_pendiente", False):
        manager._registro_retry_attempts.pop(symbol, None)
        return

    existing = manager._registro_retry_tasks.get(symbol)
    if existing and not existing.done():
        return

    attempt = manager._registro_retry_attempts.get(symbol, 0) + 1
    manager._registro_retry_attempts[symbol] = attempt

    delay = manager._registro_retry_base_delay * (
        manager._registro_retry_backoff ** (attempt - 1)
    )
    delay = min(delay, manager._registro_retry_max_delay)
    if manager._registro_retry_jitter:
        jitter = random.uniform(
            -manager._registro_retry_jitter, manager._registro_retry_jitter
        )
        delay = max(0.0, delay * (1.0 + jitter))

    reason_label = (reason or "unknown").strip() or "unknown"
    registrar_orders_retry_scheduled(symbol, reason_label)
    log.info(
        "orders.retry_schedule",
        extra=safe_extra(
            {
                "symbol": symbol,
                "attempt": attempt,
                "delay": round(delay, 3),
                "retry_reason": reason_label,
            }
        ),
    )

    async def _retry() -> None:
        next_reason: str | None = None
        try:
            await asyncio.sleep(delay)
            current = manager.ordenes.get(symbol)
            if not current or not getattr(current, "registro_pendiente", False):
                manager._registro_retry_attempts.pop(symbol, None)
                return
            try:
                await asyncio.to_thread(
                    real_orders.registrar_orden,
                    symbol,
                    current.precio_entrada,
                    current.cantidad_abierta or current.cantidad,
                    current.stop_loss,
                    current.take_profit,
                    current.estrategias_activas,
                    current.tendencia,
                    current.direccion,
                    current.operation_id,
                )
            except Exception as exc:  # pragma: no cover - se prueba vía tests específicos
                registrar_registro_error()
                log.error(
                    "orders.retry_failed",
                    extra=safe_extra(
                        {
                            "symbol": symbol,
                            "attempt": attempt,
                            "reason": type(exc).__name__,
                        }
                    ),
                )
                if should_schedule_persistence_retry(manager, exc):
                    next_reason = type(exc).__name__ or "Exception"
                else:
                    manager._registro_retry_attempts.pop(symbol, None)
                return

            current.registro_pendiente = False
            limpiar_registro_pendiente(symbol)
            manager._registro_pendiente_paused.discard(symbol)
            manager._registro_retry_attempts.pop(symbol, None)
            registrar_orden("opened")
            log.info(
                "orders.retry_success",
                extra=safe_extra({"symbol": symbol, "attempt": attempt}),
            )
        except asyncio.CancelledError:  # pragma: no cover - cancelado en shutdown
            raise
        finally:
            manager._registro_retry_tasks.pop(symbol, None)
            if next_reason:
                schedule_registro_retry(manager, symbol, reason=next_reason)

    task = loop.create_task(
        _retry(),
        name=f"registro_retry_{symbol.replace('/', '')}",
    )
    manager._registro_retry_tasks[symbol] = task
