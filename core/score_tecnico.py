"""Compatibilidad para cálculo de score técnico."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from .scoring import TechnicalScorer, ScoreBreakdown


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: Optional[float],
    momentum: Optional[float],
    slope: Optional[float],
    tendencia: str,
    direccion: str = "long",
) -> tuple[float, ScoreBreakdown]:
    """Delegado de compatibilidad que utiliza :class:`TechnicalScorer`."""
    return TechnicalScorer.calcular(df, rsi, momentum, slope, tendencia, direccion)
