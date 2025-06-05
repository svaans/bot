"""Gestión de riesgo del bot."""

from __future__ import annotations

from core.riesgo import riesgo_superado as _riesgo_superado, actualizar_perdida
from core.logger import configurar_logger


log = configurar_logger("risk", modo_silencioso=True)


class RiskManager:
    """Encapsula la lógica de control de riesgo."""

    def __init__(self, umbral: float) -> None:
        self.umbral = umbral

    def riesgo_superado(self, capital_total: float) -> bool:
        """Indica si el capital perdido supera el umbral configurado."""
        return _riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        """Registra una pérdida para ``symbol``."""
        actualizar_perdida(symbol, perdida)