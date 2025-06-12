"""Motor de estrategias para el bot de trading."""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from estrategias_entrada.gestor_entradas import evaluar_estrategias
from estrategias_entrada import gestor_entradas
from estrategias_salida.gestor_salidas import evaluar_salidas
from core.tendencia import detectar_tendencia
from core.estrategias import filtrar_por_regimen
from core.logger import configurar_logger
from ml.order_model import order_model

log = configurar_logger("engine", modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    @staticmethod
    def evaluar_entrada(symbol: str, df: pd.DataFrame, regimen: Optional[str] = None) -> Dict:
        """Obtiene la evaluación técnica de entrada para ``symbol``."""
        tendencia, _ = detectar_tendencia(symbol, df)
        resultado = evaluar_estrategias(symbol, df, tendencia)
        estrategias_activas = resultado.get("estrategias_activas", {})
        if regimen:
            estrategias_activas = filtrar_por_regimen(estrategias_activas, regimen)
            resultado["estrategias_activas"] = estrategias_activas

            probabilidad = order_model.predict_proba(estrategias_activas)
        resultado["probabilidad"] = round(float(probabilidad), 4)
        resultado["puntaje_total"] = round(
            resultado.get("puntaje_total", 0) * probabilidad, 2
        )
        return resultado

    @staticmethod
    def evaluar_salida(df: pd.DataFrame, orden: Dict) -> Dict:
        """Evalúa estrategias de salida con el estado actual de ``orden``."""
        return evaluar_salidas(orden, df)
