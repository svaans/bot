"""Funciones auxiliares relacionadas al análisis de mercado."""
from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["calcular_slope_pct", "calcular_atr_pct"]


def _to_series(values: pd.Series | pd.Index | np.ndarray | list[float]) -> pd.Series:
    if isinstance(values, pd.Series):
        return values.dropna()
    return pd.Series(values).dropna()


def calcular_slope_pct(valores: pd.Series | pd.Index | np.ndarray | list[float]) -> float:
    serie = _to_series(valores)
    if len(serie) < 2:
        return 0.0
    y = serie.astype(float).values
    x = np.arange(len(y), dtype=float)
    slope, _ = np.polyfit(x, y, 1)
    base = float(y[-1])
    if base == 0.0:
        return 0.0
    return float(slope / base)


def calcular_atr_pct(df: pd.DataFrame, periodo: int = 14) -> float:
    if df is None or len(df) < periodo + 1:
        return 0.0
    for col in ("high", "low", "close"):
        if col not in df:
            return 0.0
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.DataFrame({
        "hl": high - low,
        "hc": (high - prev_close).abs(),
        "lc": (low - prev_close).abs(),
    }).max(axis=1)
    atr = tr.rolling(window=periodo, min_periods=periodo).mean().iloc[-1]
    if pd.isna(atr):
        return 0.0
    ultimo_cierre = close.iloc[-1]
    if ultimo_cierre == 0:
        return 0.0
    return float(atr / ultimo_cierre)