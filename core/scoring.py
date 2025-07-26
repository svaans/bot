"""Funciones de scoring técnico."""
from __future__ import annotations
from typing import Optional
import pandas as pd


def calcular_score_tecnico(df: pd.DataFrame, rsi: Optional[float], momentum:
    Optional[float], slope: Optional[float], tendencia: str) ->float:
    """Agrega RSI, momentum y slope para obtener un score técnico simple."""
    score = 0.0
    if rsi is not None:
        if tendencia == 'alcista' and rsi > 50:
            score += 1
        elif tendencia == 'bajista' and rsi < 50:
            score += 1
        elif 45 <= rsi <= 55:
            score += 0.5
    if momentum is not None and momentum > 0:
        score += 1
    if slope is not None:
        if tendencia == 'alcista' and slope > 0:
            score += 1
        elif tendencia == 'bajista' and slope < 0:
            score += 1
        if slope is not None:
            if tendencia == "alcista" and slope > 0:
                score += 1
            elif tendencia == "bajista" and slope < 0:
                score += 1
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


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: Optional[float],
    momentum: Optional[float],
    slope: Optional[float],
    tendencia: str,
) -> float:
    """Compatibilidad con versiones previas."""
    return TechnicalScorer.calcular(df, rsi, momentum, slope, tendencia)
