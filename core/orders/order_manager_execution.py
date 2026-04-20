# core/orders/order_manager_execution.py — ejecución en exchange (límite vs mercado)
from __future__ import annotations

import asyncio
from typing import Any

from core.orders import real_orders
from core.orders.market_retry_executor import ExecutionResult
from core.orders.order_manager_helpers import fmt_exchange_err
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger("orders", modo_silencioso=True)


async def execute_real_order(
    manager: Any,
    side: str,
    symbol: str,
    cantidad: float,
    operation_id: str,
    entrada: dict[str, Any],
    *,
    precio: float | None = None,
) -> ExecutionResult:
    policy = manager._resolve_execution_policy(symbol, side)
    if policy == "limit" and precio is not None and manager._limit_enabled:
        try:
            resultado = await asyncio.to_thread(
                real_orders.ejecutar_orden_limit,
                symbol,
                side,
                float(precio),
                float(cantidad),
                operation_id,
                manager._limit_timeout,
                manager._limit_offset,
                manager._limit_max_retry,
            )
        except Exception as exc:  # pragma: no cover - defensivo
            log.error(
                "orders.limit_execution.error",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "side": side,
                        "reason": fmt_exchange_err(exc),
                    }
                ),
            )
            return ExecutionResult(0.0, 0.0, 0.0, "ERROR", remaining=float(cantidad))
        executed = float(resultado.get("ejecutado", 0.0))
        fee = float(resultado.get("fee", 0.0))
        pnl = float(resultado.get("pnl", 0.0))
        remaining = float(resultado.get("restante", 0.0))
        status = str(resultado.get("status") or "FILLED").upper()
        fill_avg = resultado.get("precio_fill_promedio")
        try:
            fill_avg_f = float(fill_avg) if fill_avg is not None else None
        except (TypeError, ValueError):
            fill_avg_f = None
        if fill_avg_f is not None and fill_avg_f <= 0:
            fill_avg_f = None
        if remaining > 0:
            log.warning(
                "orders.limit_execution.partial",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "side": side,
                        "remaining": remaining,
                        "operation_id": operation_id,
                    }
                ),
            )
        return ExecutionResult(
            executed,
            fee,
            pnl,
            status,
            remaining=remaining,
            precio_fill_promedio=fill_avg_f,
        )

    return await manager._market_executor.ejecutar(side, symbol, cantidad, operation_id, entrada)
