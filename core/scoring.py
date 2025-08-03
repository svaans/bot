"""Funciones de scoring técnico."""
from __future__ import annotations
from typing import Optional
import pandas as pd


PESOS_SCORE_TECNICO = {
    "RSI": 1.0,
    "Momentum": 0.5,
    "Slope": 1.0,
    "Tendencia": 1.0,
}


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: Optional[float],
    momentum: Optional[float],
    slope: Optional[float],
    tendencia: str,
) -> float:
    """Agrega RSI, momentum y slope ponderados para obtener un score técnico."""
    score = 0.0
    peso_rsi = PESOS_SCORE_TECNICO.get("RSI", 1.0)
    if rsi is not None:
        if tendencia == "alcista" and rsi > 50:
            score += peso_rsi
        elif tendencia == "bajista" and rsi < 50:
            score += peso_rsi
        elif 45 <= rsi <= 55:
            score += peso_rsi * 0.5
    if momentum is not None and momentum > 0:
        score += PESOS_SCORE_TECNICO.get("Momentum", 1.0)
    if slope is not None:
        peso_slope = PESOS_SCORE_TECNICO.get("Slope", 1.0)
        if tendencia == "alcista" and slope > 0:
            score += peso_slope
        elif tendencia == "bajista" and slope < 0:
            score += peso_slope
    return score
    
class TechnicalScorer:
    """Utilidad para calcular el puntaje técnico de forma orientada a objetos."""

    @staticmethod
    def calcular(
        df: pd.DataFrame,
        rsi: Optional[float],
        momentum: Optional[float],
        slope: Optional[float],
        tendencia: str,
    ) -> float:
        """Delegado estático al cálculo procedural."""
        return calcular_score_tecnico(df, rsi, momentum, slope, tendencia)

