from __future__ import annotations

def trade_risk(qty: float, price: float, stop_price: float, fee_pct: float = 0.0, slippage_pct: float = 0.0) -> float:
    """Return total monetary risk for a position."""
    cost_per_unit = abs(price - stop_price) + price * (fee_pct + slippage_pct)
    return qty * cost_per_unit

def total_exposure(qty: float, price: float, current_exposure: float = 0.0) -> float:
    """Return exposure after adding a new position."""
    return current_exposure + qty * price

def within_limits(
    qty: float,
    price: float,
    stop_price: float,
    risk_limit: float,
    exposure_limit: float,
    current_exposure: float,
    fee_pct: float = 0.0,
    slippage_pct: float = 0.0,
) -> bool:
    """Validate that trade risk and exposure remain under their limits."""
    r = trade_risk(qty, price, stop_price, fee_pct, slippage_pct)
    e = total_exposure(qty, price, current_exposure)
    return r <= risk_limit + 1e-9 and e <= exposure_limit + 1e-9