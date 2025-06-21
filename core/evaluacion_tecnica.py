"""Funciones de evaluación técnica utilizadas por el engine."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from estrategias_entrada.gestor_entradas import evaluar_estrategias as _eval


def evaluar_estrategias(symbol: str, df: pd.DataFrame, tendencia: str) -> Dict:
    """Evalúa las estrategias activas para ``symbol`` y ``tendencia``."""
    return _eval(symbol, df, tendencia)