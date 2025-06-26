from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SymbolState:
    tendencia: Optional[str] = None
    volatilidad: float = 0.0
    volumen: float = 0.0
    ultima_actualizacion: Optional[int] = None


@dataclass
class GlobalMarketState:
    volatilidad_global: float = 0.0
    tendencia_btc: Optional[str] = None
    dominancia_btc: float = 0.0


@dataclass
class PortfolioState:
    posiciones: Dict[str, float] = field(default_factory=dict)
    riesgo_actual: float = 0.0


class MarketContext:
    """Contexto compartido de mercado y cartera."""

    def __init__(self) -> None:
        self.symbol_state: Dict[str, SymbolState] = {}
        self.global_state = GlobalMarketState()
        self.portfolio_state = PortfolioState()
        self.historial_decisiones: List[dict] = []

    def actualizar_symbol(self, symbol: str, volatilidad: float, volumen: float, tendencia: Optional[str]) -> None:
        estado = self.symbol_state.setdefault(symbol, SymbolState())
        estado.volatilidad = volatilidad
        estado.volumen = volumen
        estado.tendencia = tendencia

    def registrar_decision(self, decision: dict) -> None:
        self.historial_decisiones.append(decision)
