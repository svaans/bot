"""Filtros simulados para símbolos de Binance."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Dict

__all__ = ["SymbolFilters", "get_symbol_filters"]


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Valores mínimos para operar en un mercado."""

    tick_size: float = 0.01
    step_size: float = 0.001
    min_notional: float = 10.0

    def to_dict(self) -> Dict[str, float]:
        return {
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "min_notional": self.min_notional,
        }


@lru_cache(maxsize=256)
def get_symbol_filters(symbol: str, cliente) -> Dict[str, float]:
    """Retorna filtros deterministas.

    Se cachea por símbolo para imitar el comportamiento del cliente real.
    """

    base = SymbolFilters()
    return base.to_dict()