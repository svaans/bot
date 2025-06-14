"""Motor de estrategias para el bot de trading."""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from estrategias_entrada.gestor_entradas import evaluar_estrategias
from estrategias_salida.gestor_salidas import evaluar_salidas
from core.tendencia import detectar_tendencia
from core.estrategias import filtrar_por_regimen
from core.logger import configurar_logger

log = configurar_logger("engine", modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    @staticmethod
    def evaluar_entrada(
        symbol: str,
        df: pd.DataFrame,
        regimen: Optional[str] = None
    ) -> Dict:
        """
        Evalúa si se cumplen condiciones para abrir una posición.

        Args:
            symbol: Símbolo del mercado (ej. "BTC/EUR").
            df: DataFrame con datos OHLCV.
            regimen: Tipo de régimen actual (lateral, alcista, bajista).

        Returns:
            Diccionario con información de entrada:
                - estrategias_activas
                - puntaje_total
                - probabilidad
                - tendencia
        """
        if not symbol or df is None or df.empty:
            log.warning("⚠️ Entrada inválida: símbolo o DataFrame vacío.")
            return {
                "estrategias_activas": {},
                "puntaje_total": 0.0,
                "probabilidad": 0.0,
                "tendencia": "desconocida",
            }

        try:
            tendencia, _ = detectar_tendencia(symbol, df)
            resultado = evaluar_estrategias(symbol, df, tendencia)

            estrategias_activas = resultado.get("estrategias_activas", {})
            if regimen:
                estrategias_activas = filtrar_por_regimen(estrategias_activas, regimen)
                resultado["estrategias_activas"] = estrategias_activas

            resultado["tendencia"] = tendencia
            resultado["probabilidad"] = 1.0  # valor fijo por ahora

            resultado["puntaje_total"] = round(
                resultado.get("puntaje_total", 0.0) * resultado["probabilidad"], 2
            )
            return resultado

        except Exception as e:
            log.error(f"❌ Error evaluando entrada para {symbol}: {e}")
            return {
                "estrategias_activas": {},
                "puntaje_total": 0.0,
                "probabilidad": 0.0,
                "tendencia": "desconocida",
            }

    @staticmethod
    def evaluar_salida(df: pd.DataFrame, orden: Dict) -> Dict:
        """
        Evalúa si se debe cerrar una orden activa.

        Args:
            df: DataFrame con datos recientes del mercado.
            orden: Diccionario con información de la orden activa.

        Returns:
            Diccionario con resultados de las estrategias de salida.
        """
        if df is None or df.empty or not orden:
            log.warning("⚠️ Evaluación de salida con datos insuficientes.")
            return {}

        try:
            return evaluar_salidas(orden, df)
        except Exception as e:
            log.error(f"❌ Error evaluando salida: {e}")
            return {}
