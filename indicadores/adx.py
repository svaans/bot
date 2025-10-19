from __future__ import annotations

import pandas as pd
from ta.trend import ADXIndicator

from indicadores.helpers import filtrar_cerradas




def calcular_adx(df: pd.DataFrame, periodo: int = 14) -> float | None:
    """Calcula el ADX utilizando la implementación optimizada de ``ta``.

    Devuelve el último valor del indicador o ``None`` si no hay datos
    suficientes, faltan columnas necesarias o las columnas no son numéricas.
    """
    df = filtrar_cerradas(df)
    columnas = {"high", "low", "close"}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None

    try:
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
    except (ValueError, TypeError):
        return None
    
    adx_indicator = ADXIndicator(
        high=high,
        low=low,
        close=close,
        window=periodo,
        fillna=False,
    )
    adx = adx_indicator.adx()
    return float(adx.iloc[-1]) if not adx.empty else None
