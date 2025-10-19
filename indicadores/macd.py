"""Implementación del indicador Moving Average Convergence Divergence (MACD)."""

from __future__ import annotations

import pandas as pd

from indicadores.helpers import filtrar_cerradas


def calcular_macd(
    df: pd.DataFrame,
    short: int = 12,
    long: int = 26,
    signal: int = 9,
) -> tuple[float | None, float | None, float | None]:
    """Calcula el MACD usando medias exponenciales (EMA).

    Se utiliza el método clásico ``EMA(short) - EMA(long)`` y una línea de señal
    calculada también con una EMA. Solo se consideran velas cerradas para evitar
    valores inestables. Devuelve ``None`` cuando no existe información
    suficiente.
    """
    df = filtrar_cerradas(df)
    if 'close' not in df.columns or len(df) < max(long, short) + signal:
        return None, None, None
    cierres = df['close'].astype(float)
    ema_short = cierres.ewm(span=short, adjust=False).mean()
    ema_long = cierres.ewm(span=long, adjust=False).mean()
    macd = ema_short - ema_long
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histograma = macd - signal_line
    
    if macd.empty or signal_line.empty or histograma.empty:
        return None, None, None

    return (
        float(macd.iloc[-1]),
        float(signal_line.iloc[-1]),
        float(histograma.iloc[-1]),
    )
