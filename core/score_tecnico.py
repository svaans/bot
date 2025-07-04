"""Compatibilidad para cálculo de score técnico."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from .scoring import calcular_score_tecnico as _calcular


def calcular_score_tecnico(df: pd.DataFrame, rsi: Optional[float], momentum:
    Optional[float], slope: Optional[float], tendencia: str) ->float:
    """Función de compatibilidad que delega en :mod:`core.scoring`."""
    return _calcular(df, rsi, momentum, slope, tendencia)
