"""Utilidades para calcular correlaciones entre series temporales."""

from __future__ import annotations

import pandas as pd


def calcular_correlacion(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    columna: str = "close",
    ventana: int = 30,
    align_posicional: bool = False,
) -> float | None:
    """Calcula la correlación de Pearson entre dos series de precios.

    Esta función se mantiene por compatibilidad, pero delega el cálculo en
    :func:`correlacion_series` para favorecer la reutilización.

    Parameters
    ----------
    df1, df2:
        ``DataFrame`` con las series a comparar.
    columna:
        Nombre de la columna a usar. Si se especifica ``"returns"`` se
        calcularán los retornos simples a partir de ``close``.
    ventana:
        Número máximo de muestras recientes a considerar.
    align_posicional:
        Si es ``True``, ignora los índices al alinear las series y correlaciona
        por posición.
    """
    if columna == "returns":
        serie1 = df1["close"].pct_change().dropna()
        serie2 = df2["close"].pct_change().dropna()
    else:
        if columna not in df1 or columna not in df2:
            return None
        serie1 = df1[columna]
        serie2 = df2[columna]

    return correlacion_series(serie1, serie2, ventana, align_posicional)


def correlacion_series(
    a: pd.Series,
    b: pd.Series,
    ventana: int = 30,
    align_posicional: bool = False,
) -> float | None:
    """Correlación de Pearson entre dos ``pd.Series``.

    Parameters
    ----------
    a, b:
        Series de precios o retornos a comparar.
    ventana:
        Número máximo de muestras recientes a considerar. Debe ser al menos 2.
    align_posicional:
        Si es ``True`` se alinean las series por posición en lugar de por
        índice.
    """

    if ventana < 2:
        raise ValueError("ventana debe ser >= 2")

    if align_posicional:
        serie1 = a.tail(ventana).reset_index(drop=True)
        serie2 = b.tail(ventana).reset_index(drop=True)
    else:
        serie1 = a.tail(ventana)
        serie2 = b.tail(ventana)
    if len(serie1) < 2 or len(serie2) < 2:
        return None
    r = serie1.corr(serie2)
    return None if pd.isna(r) else float(r)
