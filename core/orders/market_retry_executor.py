"""Ejecución resiliente de órdenes de mercado."""
from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass
from typing import Any

from core.metrics import registrar_market_retry_exhausted
from core.orders import real_orders
from core.utils.logger import log_decision


@dataclass(slots=True)
class ExecutionResult:
    """Resultado normalizado de una ejecución de orden."""

    executed: float
    fee: float
    pnl: float
    status: str
    remaining: float = 0.0


class MarketRetryExecutor:
    """Coordina las operaciones de mercado con reintentos y métricas."""

    def __init__(self, *, log, bus) -> None:
        self.log = log
        self.bus = bus
        self._max_attempts = int(os.getenv("ORDERS_MARKET_MAX_RETRIES", "5"))
        self._backoff = float(os.getenv("ORDERS_MARKET_BACKOFF_SECONDS", "1.0"))
        self._backoff_multiplier = float(
            os.getenv("ORDERS_MARKET_BACKOFF_MULTIPLIER", "1.5")
        )
        self._backoff_cap = float(
            os.getenv("ORDERS_MARKET_BACKOFF_MAX_SECONDS", "30.0")
        )
        self._no_progress_limit = int(
            os.getenv("ORDERS_MARKET_MAX_NO_PROGRESS", "3")
        )

    async def ejecutar(
        self,
        side: str,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict[str, Any],
    ) -> ExecutionResult:
        if side == "sell":
            return await self._ejecutar_market_sell(symbol, cantidad, operation_id, entrada)
        return await self._ejecutar_market_buy(symbol, cantidad, operation_id, entrada)

    async def _ejecutar_market_sell(
        self,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict[str, Any],
    ) -> ExecutionResult:
        intentos = 0
        while True:
            intentos += 1
            try:
                resp = await asyncio.to_thread(
                    real_orders._market_sell_retry, symbol, cantidad, operation_id
                )
                salida = {
                    "ejecutado": float(resp.get("ejecutado", 0.0)),
                    "fee": float(resp.get("fee", 0.0)),
                    "pnl": float(resp.get("pnl", 0.0)),
                }
                log_decision(
                    self.log,
                    "_market_sell",
                    operation_id,
                    entrada,
                    {},
                    "execute",
                    salida,
                )
                status = str(resp.get("status") or "FILLED").upper()
                remaining = float(resp.get("restante", 0.0))
                return ExecutionResult(
                    executed=salida["ejecutado"],
                    fee=salida["fee"],
                    pnl=salida["pnl"],
                    status=status,
                    remaining=remaining,
                )
            except Exception as exc:  # pragma: no cover - defensivo
                self.log.error(
                    "❌ Error ejecutando venta en %s (intento %s/%s): %s",
                    symbol,
                    intentos,
                    self._max_attempts,
                    exc,
                    extra={"event": "market_sell_retry_error", "symbol": symbol},
                )
                if intentos >= self._max_attempts:
                    await self._manejar_exhausted("sell", symbol, operation_id)
                    return 0.0, 0.0, 0.0
                await asyncio.sleep(self._market_retry_sleep(intentos))

    async def _ejecutar_market_buy(
        self,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict[str, Any],
    ) -> ExecutionResult:
        restante = cantidad
        total = total_fee = total_pnl = 0.0
        intentos = 0
        sin_progreso = 0
        while restante > 0:
            intentos += 1
            restante_previo = restante
            try:
                resp = await asyncio.to_thread(
                    real_orders.ejecutar_orden_market, symbol, restante, operation_id
                )
            except Exception as exc:  # pragma: no cover - defensivo
                self.log.error(
                    "❌ Error ejecutando compra en %s (intento %s/%s): %s",
                    symbol,
                    intentos,
                    self._max_attempts,
                    exc,
                    extra={"event": "market_buy_retry_error", "symbol": symbol},
                )
                sin_progreso += 1
                if self._should_break_retry(intentos, sin_progreso):
                    await self._manejar_exhausted("buy", symbol, operation_id)
                    break
                await asyncio.sleep(self._market_retry_sleep(intentos))
                continue

            ejecutado = float(resp.get("ejecutado", 0.0))
            total += ejecutado
            restante = float(resp.get("restante", 0.0))
            total_fee += float(resp.get("fee", 0.0))
            total_pnl += float(resp.get("pnl", 0.0))

            if math.isclose(restante, restante_previo, rel_tol=1e-09, abs_tol=1e-12):
                sin_progreso += 1
            else:
                sin_progreso = 0

            if resp.get("status") != "PARTIAL" or restante < resp.get("min_qty", 0):
                break

            if self._should_break_retry(intentos, sin_progreso):
                await self._manejar_exhausted("buy", symbol, operation_id)
                break

            await asyncio.sleep(self._market_retry_sleep(intentos))
        salida = {"ejecutado": total, "fee": total_fee, "pnl": total_pnl}
        log_decision(
            self.log,
            "_market_buy",
            operation_id,
            entrada,
            {},
            "execute",
            salida,
        )
        status = "FILLED" if restante <= 1e-8 else "PARTIAL"
        return ExecutionResult(
            executed=total,
            fee=total_fee,
            pnl=total_pnl,
            status=status,
            remaining=max(restante, 0.0),
        )

    def _market_retry_sleep(self, attempt: int) -> float:
        base = max(self._backoff, 0.0)
        if base <= 0:
            return 0.0
        multiplier = max(self._backoff_multiplier, 1.0)
        delay = base * (multiplier ** max(attempt - 1, 0))
        return min(delay, self._backoff_cap)

    def _should_break_retry(self, attempts: int, no_progress: int) -> bool:
        if attempts >= self._max_attempts:
            return True
        return self._no_progress_limit > 0 and no_progress >= self._no_progress_limit

    async def _manejar_exhausted(
        self, side: str, symbol: str, operation_id: str
    ) -> None:
        registrar_market_retry_exhausted(side, symbol)
        self.log.error(
            "⛔ Reintentos de orden de mercado agotados",
            extra={
                "event": "market_retry_exhausted",
                "side": side,
                "symbol": symbol,
                "operation_id": operation_id,
                "max_attempts": self._max_attempts,
            },
        )
        if self.bus:
            await self.bus.publish(
                "notify",
                {
                    "mensaje": (
                        f"⛔ No se pudo ejecutar orden {side} en {symbol}: "
                        f"{self._max_attempts} reintentos fallidos"
                    ),
                    "tipo": "CRITICAL",
                    "operation_id": operation_id,
                },
            )

    def update_bus(self, bus) -> None:
        """Permite actualizar el bus de notificaciones dinámicamente."""

        self.bus = bus
