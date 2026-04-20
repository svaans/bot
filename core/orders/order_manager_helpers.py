# core/orders/order_manager_helpers.py — utilidades sin estado para OrderManager
from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Mapping
from typing import Any

from binance_api.cliente import BinanceClient, fetch_balance_async
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import is_valid_number


def fmt_exchange_err(exc: BaseException | None, *, limit: int = 500) -> str:
    """Acorta mensajes de error (p. ej. CCXT) para logs y notificaciones."""

    return format_exception_for_log(exc, limit)


async def fetch_balance_non_blocking(cliente: Any | None) -> Mapping[str, Any]:
    """Obtiene el balance evitando bloquear el loop principal."""

    if cliente is None:
        return await fetch_balance_async(None)

    fetch_balance = getattr(cliente, "fetch_balance", None)
    if callable(fetch_balance):
        if inspect.iscoroutinefunction(fetch_balance):
            return await fetch_balance()
        resultado = await asyncio.to_thread(fetch_balance)
        if inspect.isawaitable(resultado):
            return await resultado
        return resultado

    fetch_balance_async_attr = getattr(cliente, "fetch_balance_async", None)
    if callable(fetch_balance_async_attr):
        return await fetch_balance_async_attr()

    if isinstance(cliente, BinanceClient):
        return await fetch_balance_async(cliente)

    return {}


def backtest_slippage_bps() -> float:
    try:
        v = float(os.getenv("BACKTEST_SLIPPAGE_BPS", "0") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, v)


def sim_aplicar_slippage_entrada_salida(
    precio: float,
    direccion: str,
    *,
    es_entrada: bool,
) -> float:
    """Ajusta precio en backtest: long compra más caro / vende más barato; short al revés."""

    bps = backtest_slippage_bps()
    if bps <= 0 or not is_valid_number(precio) or precio <= 0:
        return precio
    k = bps / 10000.0
    d = (direccion or "long").lower()
    short = d in ("short", "venta")
    if not short:
        factor = (1.0 + k) if es_entrada else (1.0 - k)
    else:
        factor = (1.0 - k) if es_entrada else (1.0 + k)
    return float(precio * factor)


def is_short_direction(direccion: str | None) -> bool:
    d = (direccion or "").lower()
    return d in ("short", "venta", "sell")


def exchange_side_open_position(direccion: str | None) -> str:
    """Spot/margen típico: abrir o aumentar long -> ``buy``; short -> ``sell``."""

    return "sell" if is_short_direction(direccion) else "buy"


def exchange_side_reduce_position(direccion: str | None) -> str:
    """Reducir long -> ``sell``; cubrir short -> ``buy``."""

    return "buy" if is_short_direction(direccion) else "sell"
