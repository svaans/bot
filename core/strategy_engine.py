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
    def evaluar_entrada(symbol: str, df: pd.DataFrame) -> Dict:
        """Obtiene la evaluación técnica de entrada para ``symbol``."""
        tendencia, _ = detectar_tendencia(symbol, df)
        return evaluar_estrategias(symbol, df, tendencia)

    @staticmethod
    def evaluar_salida(df: pd.DataFrame, orden: Dict) -> Dict:
        """Evalúa estrategias de salida con el estado actual de ``orden``."""
        return evaluar_salidas(orden, df)