"""Filtros de símbolo para Binance (CCXT o valores por defecto)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

__all__ = ["SymbolFilters", "get_symbol_filters"]


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Valores mínimos por defecto cuando no hay mercado CCXT."""

    tick_size: float = 0.01
    step_size: float = 0.001
    min_notional: float = 10.0

    def to_dict(self) -> Dict[str, float]:
        return {
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "min_notional": self.min_notional,
            "min_qty": 0.0,
        }


def _from_ccxt_market(market: dict[str, Any]) -> Dict[str, float] | None:
    try:
        lim = market.get("limits") or {}
        min_qty = float((lim.get("amount") or {}).get("min") or 0.0)
        min_notional = float((lim.get("cost") or {}).get("min") or 0.0)
        prec = market.get("precision") or {}
        tick_size = float(prec.get("price") or 0.0) or 0.01
        step_size = 0.001
        info = market.get("info") or {}
        for f in info.get("filters") or []:
            if f.get("filterType") == "LOT_SIZE":
                try:
                    step_size = float(f.get("stepSize") or step_size)
                except (TypeError, ValueError):
                    pass
                break
        return {
            "tick_size": tick_size,
            "step_size": step_size,
            "min_notional": min_notional,
            "min_qty": min_qty,
        }
    except Exception:
        return None


def get_symbol_filters(symbol: str, cliente: Any) -> Dict[str, float]:
    """Filtros del mercado vía CCXT si ``cliente`` expone ``market``/``markets``."""

    if cliente is not None:
        markets = getattr(cliente, "markets", None)
        market_fn = getattr(cliente, "market", None)
        if markets and callable(market_fn):
            try:
                m = market_fn(symbol)
                got = _from_ccxt_market(m)
                if got is not None:
                    return got
            except Exception:
                pass

    base = SymbolFilters()
    return base.to_dict()
