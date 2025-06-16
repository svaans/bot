"""Motor de estrategias para el bot de trading."""

from __future__ import annotations

from typing import Dict

from core.pesos import gestor_pesos
from core.utils import validar_dataframe
from core.adaptador_umbral import calcular_umbral_adaptativo

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
        tendencia: str | None = None,
        config: dict | None = None,
        pesos_symbol: dict | None = None,
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
        if not symbol or not validar_dataframe(df, ["close", "high", "low", "volume"]):
            log.warning("⚠️ Entrada inválida: símbolo o DataFrame inválido.")
            return {
                "estrategias_activas": {},
                "puntaje_total": 0.0,
                "probabilidad": 0.0,
                "tendencia": "desconocida",
            }

        try:
            if pesos_symbol is None:
                pesos_symbol = gestor_pesos.obtener_pesos_symbol(symbol)

            # Detectar tendencia si no viene dada
            if tendencia is None:
                tendencia, _ = detectar_tendencia(symbol, df)
                log.info(f"📊 [{symbol}] Tendencia detectada: {tendencia}")
            else:
                log.debug(f"[{symbol}] Tendencia previa usada: {tendencia}")

            # Evaluar estrategias
            resultado = evaluar_estrategias(symbol, df, tendencia)
            estrategias_activas = resultado.get("estrategias_activas", {})

            log.info(f"🔍 [{symbol}] Estrategias activas antes del filtro: {list(estrategias_activas.keys())}")

            dispersion = resultado.get("diversidad", 0) / max(len(estrategias_activas) or 1, 1)
            puntaje_total = resultado.get("puntaje_total", 0.0)
            umbral = calcular_umbral_adaptativo(
                symbol,
                df,
                estrategias_activas,
                pesos_symbol,
                persistencia=0.0,
                config=config,
            )
            max_peso = sum(pesos_symbol.values()) or 1.0
            puntaje_rel = puntaje_total / max_peso
            score_tecnico = puntaje_total / umbral if umbral else 0.0
            prob = (dispersion + puntaje_rel + score_tecnico) / 3
            if config and config.get("modo_agresivo"):
                prob *= 1.1
            prob = round(min(1.0, max(0.0, prob)), 2)

            resultado["tendencia"] = tendencia
            resultado["probabilidad"] = prob
            resultado["puntaje_total"] = round(puntaje_total, 2)

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
