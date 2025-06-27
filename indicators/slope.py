# slope.py
import pandas as pd
import numpy as np
from fast_indicators import slope as _slope_fast

def calcular_slope(df: pd.DataFrame, periodo: int = 5) -> float:
    if "close" not in df or len(df) < periodo:
        return 0.0

    y = df["close"].tail(periodo).to_numpy()
    x = np.arange(len(y))
    if len(y) < 2:
        return 0.0

    slope, _ = np.polyfit(x, y, 1)
    return float(slope)


def calcular_slope_fast(df: pd.DataFrame, periodo: int = 5) -> float:
    """Versión acelerada de :func:`calcular_slope` usando la extensión en C++."""
    if "close" not in df or len(df) < periodo:
        return 0.0
    close = df["close"].to_numpy(dtype=float)
    return float(_slope_fast(close, periodo))
