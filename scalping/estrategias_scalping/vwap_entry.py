"""Entrada en rebotes sobre el VWAP."""

import pandas as pd
from utils_scalping.indicadores import vwap


def evaluar(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
    vw = vwap(df)
    close_prev = df["close"].iloc[-2]
    close_now = df["close"].iloc[-1]
    return close_prev < vw.iloc[-2] and close_now > vw.iloc[-1]