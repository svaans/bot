import pandas as pd
import numpy as np


def calcular_volumen_normalizado(df: pd.DataFrame, ventana: int = 20) -> float:
    """Normaliza el volumen actual en ``[0, 1]``.

    Usa únicamente velas cerradas y evita valores no finitos."""
    if not isinstance(df, pd.DataFrame) or 'volume' not in df:
        return 0.0

    df_filtrado = df[df['is_closed']] if 'is_closed' in df else df
    if df_filtrado.empty:
        return 0.0

    serie = df_filtrado['volume'].astype(float).tail(ventana)
    vol_actual = serie.iloc[-1]
    max_vol = serie.max()
    if max_vol == 0 or not np.isfinite(vol_actual):
        return 0.0
    vol_norm = vol_actual / max_vol
    if not np.isfinite(vol_norm):
        return 0.0
    return float(np.clip(vol_norm, 0.0, 1.0))


def calcular_volumen_alto(df: pd.DataFrame, factor: float=1.5, ventana: int=20
    ) ->bool:
    if 'volume' not in df or len(df) < ventana:
        return False
    volumen_actual = df['volume'].iloc[-1]
    volumen_promedio = df['volume'].iloc[-ventana:-1].mean()
    return volumen_actual > volumen_promedio * factor


def verificar_volumen_suficiente(df: pd.DataFrame, factor: float=0.6,
    ventana: int=20) ->bool:
    """Comprueba si el volumen actual es suficiente en relación al promedio.

    Retorna ``True`` cuando el volumen de la última vela es al menos ``factor``
    veces el volumen medio de las ``ventana`` velas previas.
    """
    if 'volume' not in df or len(df) < ventana:
        return True
    volumen_actual = df['volume'].iloc[-1]
    volumen_promedio = df['volume'].iloc[-ventana:-1].mean()
    return volumen_actual >= volumen_promedio * factor
