"""Funciones de evaluación técnica utilizadas por el engine."""
from __future__ import annotations
from typing import Dict
import pandas as pd
from core.strategies.entry import gestor_entradas


async def evaluar_estrategias(symbol: str, df: pd.DataFrame, tendencia: str) -> Dict:
    """Evalúa las estrategias activas para ``symbol`` y ``tendencia``."""
    return await gestor_entradas.evaluar_estrategias(symbol, df, tendencia)
