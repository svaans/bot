from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal, InvalidOperation


@dataclass
class MarketInfo:
    """Exchange market limits for a trading pair."""
    tick_size: float
    step_size: float
    min_notional: float


def _to_decimal(value: float) -> Decimal:
    """Convierte a Decimal vía str para evitar errores de representación binaria."""
    return Decimal(str(value))


def _round_price(price: float, tick: float, side: str = "buy") -> float:
    """Redondea ``price`` al múltiplo de ``tick`` más cercano según ``side``.

    Usa ``Decimal`` para evitar errores de representación flotante en ticks
    pequeños (< 0.001), coherente con :mod:`core.risk.level_validators`.
    """
    if tick <= 0:
        return price
    try:
        dec_price = _to_decimal(price)
        dec_tick = _to_decimal(tick)
        if side in ("sell", "short"):
            rounded = (dec_price / dec_tick).to_integral_value(rounding=ROUND_CEILING)
        else:
            rounded = (dec_price / dec_tick).to_integral_value(rounding=ROUND_DOWN)
        return float(rounded * dec_tick)
    except InvalidOperation:
        # Fallback numérico si Decimal falla (tick o price muy extremos)
        factor = price / tick
        if side in ("sell", "short"):
            return math.ceil(factor) * tick
        return math.floor(factor) * tick


def _round_qty(qty: float, step: float) -> float:
    """Redondea ``qty`` al múltiplo de ``step`` inferior usando ``Decimal``."""
    if step <= 0:
        return qty
    try:
        dec_qty = _to_decimal(qty)
        dec_step = _to_decimal(step)
        rounded = (dec_qty / dec_step).to_integral_value(rounding=ROUND_DOWN)
        return float(rounded * dec_step)
    except InvalidOperation:
        return math.floor(qty / step) * step

def apply_exchange_limits(price: float, qty: float, market: MarketInfo, side: str = "buy") -> tuple[float, float, float]:
    """Round price/qty and ensure ``min_notional``.

    ``price`` is rounded to ``tick_size`` respecting the order ``side`` while
    ``qty`` uses ``step_size``. After rounding the notional value is checked
    against ``min_notional`` and ``qty`` is increased if needed.
    Returns the adjusted ``price``, ``qty`` and resulting ``notional``.
    """
    price = _round_price(price, market.tick_size, side)
    qty = _round_qty(qty, market.step_size)
    notional = price * qty
    if market.min_notional and notional < market.min_notional and market.step_size > 0:
        qty = math.ceil(market.min_notional / price / market.step_size) * market.step_size
        notional = price * qty
    return price, qty, notional

def size_order(
    price: float,
    stop_price: float,
    market: MarketInfo,
    risk_limit: float,
    exposure_limit: float,
    current_exposure: float,
    fee_pct: float = 0.0,
    slippage_pct: float = 0.0,
    side: str = "buy",
) -> tuple[float, float]:
    """Size an order honoring risk and exposure limits.

    The position size is computed using ``risk_limit`` and ``exposure_limit``.
    Fees and slippage are incorporated in the risk calculation before deciding
    the quantity. After rounding to exchange limits the function verifies again
    that both risk per trade and total exposure remain below their limits.
    """
    cost_per_unit = abs(price - stop_price) + price * (fee_pct + slippage_pct)
    if cost_per_unit <= 0:
        return 0.0, 0.0
    max_qty_risk = risk_limit / cost_per_unit
    max_qty_exposure = max(0.0, (exposure_limit - current_exposure) / price)
    qty = max(0.0, min(max_qty_risk, max_qty_exposure))
    if qty <= 0:
        return price, 0.0
    price, qty, notional = apply_exchange_limits(price, qty, market, side)
    cost_per_unit = abs(price - stop_price) + price * (fee_pct + slippage_pct)
    risk = qty * cost_per_unit
    exposure = current_exposure + notional
    if risk > risk_limit + 1e-9 or exposure > exposure_limit + 1e-9:
        return price, 0.0
    return price, qty