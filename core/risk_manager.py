"""Gestión de riesgo del bot."""

from __future__ import annotations

import numpy as np
from core.logger import configurar_logger
from core.riesgo import riesgo_superado as _riesgo_superado, actualizar_perdida
from core.reporting import reporter_diario

log = configurar_logger("risk", modo_silencioso=True)


class RiskManager:
    """Encapsula la lógica de control de riesgo del bot."""

    def __init__(self, umbral: float) -> None:
        self.umbral = umbral

    def riesgo_superado(self, capital_total: float) -> bool:
        """Indica si el capital perdido supera el umbral configurado."""
        return _riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        """Registra una pérdida para ``symbol``."""
        if perdida > 0:
            actualizar_perdida(symbol, perdida)

    def ajustar_umbral(self, segun_metricas: dict) -> None:
        """
        Modifica ``self.umbral`` de acuerdo al desempeño reciente.

        Espera un diccionario con las claves:
        - ganancia_semana (float): ganancia semanal acumulada
        - drawdown (float): pérdida máxima registrada en la semana

        Lógica:
        - Si ganancia semanal > 5%, aumenta el umbral hasta un máximo del 0.5
        - Si drawdown < -5%, reduce el umbral hasta un mínimo del 0.01
        """
        if not isinstance(segun_metricas, dict):
            log.warning("⚠️ Métricas de riesgo no proporcionadas como diccionario")
            return

        ganancia = segun_metricas.get("ganancia_semana", 0.0)
        drawdown = segun_metricas.get("drawdown", 0.0)

        if not isinstance(ganancia, (int, float)) or not isinstance(drawdown, (int, float)):
            log.warning("⚠️ Métricas inválidas para ajuste de riesgo")
            return
        if np.isnan(ganancia) or np.isnan(drawdown):
            log.warning("⚠️ Métricas NaN detectadas")
            return

        umbral_anterior = self.umbral

        if ganancia > 0.05:
            self.umbral = round(min(0.5, self.umbral * 1.05), 4)
        elif drawdown < -0.05:
            self.umbral = round(max(0.01, self.umbral * 0.9), 4)

        if self.umbral != umbral_anterior:
            log.info(f"🔧 Umbral ajustado de {umbral_anterior:.4f} → {self.umbral:.4f}")

    def multiplicador_kelly(self, n_trades: int = 10) -> float:
        """
        Calcula un factor de ajuste para la fracción de Kelly.

        Se basa en los últimos ``n_trades`` y su retorno.
        - Si no hay datos o fallan los cálculos, retorna 1.0 (sin ajuste).
        - El resultado se limita entre 0.5 y 1.5 para evitar extremos.
        """
        try:
            operaciones = []
            for trades in reporter_diario.ultimas_operaciones.values():
                operaciones.extend(trades[-n_trades:])

            operaciones = operaciones[-n_trades:]
            if not operaciones:
                return 1.0

            retornos = [
                float(o.get("retorno_total", 0.0))
                for o in operaciones
                if isinstance(o.get("retorno_total"), (int, float)) and not np.isnan(o.get("retorno_total"))
            ]
            if not retornos:
                return 1.0

            promedio = sum(retornos) / len(retornos)
            factor = round(max(0.5, min(1.5, 1 + promedio)), 3)
            log.debug(f"🔧 Multiplicador Kelly calculado: {factor:.3f}")
            return factor
        except Exception as e:
            log.warning(f"⚠️ Error calculando multiplicador Kelly: {e}")
            return 1.0

    def factor_volatilidad(
        self,
        volatilidad_actual: float,
        volatilidad_media: float,
        umbral: float = 2.0,
    ) -> float:
        """
        Devuelve un factor reductor si la volatilidad actual es anómala.

        Si la volatilidad actual excede en más de ``umbral`` veces a la media,
        se penaliza la exposición reduciendo el tamaño de la posición.
        """
        if (
            not isinstance(volatilidad_actual, (int, float)) or
            not isinstance(volatilidad_media, (int, float)) or
            volatilidad_actual <= 0 or volatilidad_media <= 0
        ):
            return 1.0

        exceso = volatilidad_actual / (volatilidad_media * umbral)
        if exceso <= 1.0:
            return 1.0

        factor = 1.0 / exceso
        factor = round(max(0.5, min(1.0, factor)), 3)
        log.info(f"🌪️ Volatilidad excesiva, aplicando factor de reducción: {factor}")
        return factor
