"""Cálculo de líneas Tenkan-sen y Kijun-sen del indicador Ichimoku."""

from __future__ import annotations

import pandas as pd

from indicadores.helpers import filtrar_cerradas


def calcular_ichimoku_breakout(
    df: pd.DataFrame,
    tenkan_period: int = 9,
    kijun_period: int = 26,
) -> tuple[float | None, float | None]:
    """Calcula Tenkan-sen y Kijun-sen usando máximos y mínimos móviles.

    Se aplican ventanas móviles sobre los máximos y mínimos de velas cerradas.
    El resultado coincide con la definición estándar del indicador Ichimoku y
    devuelve ``None`` cuando no existe historial suficiente.
    """

    df = filtrar_cerradas(df)
    if {'high', 'low'} - set(df.columns):
        return None, None


    min_longitud = max(tenkan_period, kijun_period)
    if len(df) < min_longitud:
        return None, None
    altos = df['high'].astype(float)
    bajos = df['low'].astype(float)
    nine_high = altos.rolling(window=tenkan_period, min_periods=tenkan_period).max()
    nine_low = bajos.rolling(window=tenkan_period, min_periods=tenkan_period).min()
    tenkan_sen = (nine_high + nine_low) / 2
    twenty_six_high = altos.rolling(window=kijun_period, min_periods=kijun_period).max()
    twenty_six_low = bajos.rolling(window=kijun_period, min_periods=kijun_period).min()
    kijun_sen = (twenty_six_high + twenty_six_low) / 2
    if tenkan_sen.empty or kijun_sen.empty:
        return None, None

    return float(tenkan_sen.iloc[-1]), float(kijun_sen.iloc[-1])
