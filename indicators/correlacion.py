"""Utilidades para calcular correlaciones entre series temporales."""

from __future__ import annotations

import pandas as pd


def calcular_correlacion(
    df1: pd.DataFrame, df2: pd.DataFrame, columna: str = "close", ventana: int = 30
) -> float | None:
    """Calcula la correlación de Pearson entre dos series de precios.

    Esta función se mantiene por compatibilidad, pero delega el cálculo en
    :func:`correlacion_series` para favorecer la reutilización.
    """
    if columna not in df1 or columna not in df2:
        return None
    return correlacion_series(df1[columna], df2[columna], ventana)


def correlacion_series(
    a: pd.Series, b: pd.Series, ventana: int = 30
) -> float | None:
    """Correlación de Pearson entre dos ``pd.Series``.

    Parameters
    ----------
    a, b:
        Series de precios o retornos a comparar.
    ventana:
        Número máximo de muestras recientes a considerar. Si alguna de las
        series tiene menos de dos valores tras aplicar la ventana, se devuelve
        ``None``.
    """

    serie1 = a.tail(ventana)
    serie2 = b.tail(ventana)
    if len(serie1) < 2 or len(serie2) < 2:
        return None
    return float(serie1.corr(serie2))
