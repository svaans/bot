# core/orders/order_manager_reconcile_tasks.py — bucle periódico reconciliar_trades (background)
from __future__ import annotations

import asyncio
from typing import Any

from core.orders import real_orders
from core.orders.order_manager_helpers import fmt_exchange_err as _fmt_exchange_err
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger("orders", modo_silencioso=True)


def log_reconcile_report_divergences(divergencias: list[dict[str, Any]]) -> None:
    """Callback pasado a :func:`real_orders.reconciliar_trades_binance` (reporter)."""
    if not divergencias:
        log.debug("orders.reconcile_trades.clean")
        return
    for divergence in divergencias:
        log.warning(
            "orders.reconcile_trades.divergence",
            extra=safe_extra(divergence),
        )


async def reconcile_trades_loop(manager: Any) -> None:
    """Tarea en background: invoca periódicamente la reconciliación de trades vs SQLite."""
    while True:
        try:
            divergencias = await asyncio.to_thread(
                real_orders.reconciliar_trades_binance,
                None,
                manager._reconcile_limit,
                False,
                log_reconcile_report_divergences,
            )
            if divergencias:
                log.warning(
                    "orders.reconcile_trades.detected",
                    extra=safe_extra(
                        {
                            "count": len(divergencias),
                        }
                    ),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensivo
            log.error(
                "orders.reconcile_trades.error",
                extra=safe_extra({"error": _fmt_exchange_err(exc)}),
            )
        try:
            await asyncio.sleep(manager._reconcile_interval)
        except asyncio.CancelledError:
            raise
