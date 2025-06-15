"""Motor de estrategias para el bot de trading."""

from __future__ import annotations

from typing import Dict

import pandas as pd

from estrategias_entrada.gestor_entradas import evaluar_estrategias
from estrategias_salida.gestor_salidas import evaluar_salidas
from core.tendencia import detectar_tendencia
from core.logger import configurar_logger

log = configurar_logger("engine", modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    @staticmethod
    def evaluar_entrada(
        symbol: str,
        df: pd.DataFrame,
    ) -> Dict:
        """
        Evalúa si se cumplen condiciones para abrir una posición.

        Args:
            symbol: Símbolo del mercado (ej. "BTC/EUR").
            df: DataFrame con datos OHLCV.

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
            # Detectar tendencia
            tendencia, _ = detectar_tendencia(symbol, df)
            log.info(f"📊 [{symbol}] Tendencia detectada: {tendencia}")

            # Evaluar estrategias
            resultado = evaluar_estrategias(symbol, df, tendencia)
            estrategias_activas = resultado.get("estrategias_activas", {})

            log.info(f"🔍 [{symbol}] Estrategias activas antes del filtro: {list(estrategias_activas.keys())}")

            # Asignar metadatos
            resultado["tendencia"] = tendencia
            resultado["probabilidad"] = 1.0  # Fijo por ahora

            resultado["puntaje_total"] = round(
                resultado.get("puntaje_total", 0.0) * resultado["probabilidad"], 2
            )

            log.info(f"📈 [{symbol}] Puntaje total calculado: {resultado['puntaje_total']}")
            log.info(f"✅ [{symbol}] Estrategias finales: {list(estrategias_activas.keys())}")

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
