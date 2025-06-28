import pandas as pd
import numpy as np
from fast_indicators import atr as _atr_fast
from core.utils.cache_indicadores import cached_indicator

def calcular_atr(df: pd.DataFrame, periodo: int = 14) -> float:
    """Calcula el Average True Range (ATR) usando el método de Wilder."""

    columnas = {"high", "low", "close"}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None
    df = df.copy()
    cierre_prev = df["close"].shift()

    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - cierre_prev).abs()
    tr3 = (df["low"] - cierre_prev).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()

    return atr.iloc[-1] if not atr.empty else None


def calcular_atr_fast(df: pd.DataFrame, periodo: int = 14) -> float:
    """Versión acelerada de :func:`calcular_atr` usando la extensión en C++."""
    columnas = {"high", "low", "close"}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    return float(_atr_fast(high, low, close, periodo))