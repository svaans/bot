# slope.py
import pandas as pd
import numpy as np
from fast_indicators import slope as _slope_fast

try:  # pragma: no cover - optional rust extension
    from fast_indicators_rust import slope as _slope_rust
    HAS_RUST = True
except Exception:  # pragma: no cover - missing rust module
    _slope_rust = None
    HAS_RUST = False

def calcular_slope(df: pd.DataFrame, periodo: int = 5) -> float:
    if "close" not in df or len(df) < periodo:
        return 0.0

    y = df["close"].tail(periodo).to_numpy(dtype=float)
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
    if HAS_RUST:
        return float(_slope_rust(close, periodo))
    return float(_slope_fast(close, periodo))
