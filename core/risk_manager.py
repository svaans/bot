"""Gestión de riesgo del bot."""

from __future__ import annotations

from core.riesgo import riesgo_superado as _riesgo_superado, actualizar_perdida
import numpy as np
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

    def ajustar_umbral(self, segun_metricas: dict) -> None:
        """Modifica ``self.umbral`` de acuerdo al desempeño reciente.

        ``segun_metricas`` debe contener ``ganancia_semana`` y ``drawdown``.
        Si la ganancia semanal es mayor a 5% se incrementa ligeramente el
        umbral. Si el drawdown es negativo por debajo de -5%, se reduce.
        """
        if not isinstance(segun_metricas, dict):
            return

        ganancia = segun_metricas.get("ganancia_semana", 0.0)
        drawdown = segun_metricas.get("drawdown", 0.0)
        if not isinstance(ganancia, (int, float)) or not isinstance(drawdown, (int, float)):
            log.warning("⚠️ Métricas inválidas para ajuste de riesgo")
            return
        if np.isnan(ganancia) or np.isnan(drawdown):
            log.warning("⚠️ Métricas NaN para ajuste de riesgo")
            return

        anterior = self.umbral

        if ganancia > 0.05:
            self.umbral = round(min(0.5, self.umbral * 1.05), 4)
        elif drawdown < -0.05:
            self.umbral = round(max(0.01, self.umbral * 0.9), 4)

        if self.umbral != anterior:
            log.info(f"🔧 Umbral ajustado de {anterior:.4f} a {self.umbral:.4f}")