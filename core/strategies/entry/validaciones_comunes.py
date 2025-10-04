from __future__ import annotations
import pandas as pd
from indicadores.helpers import get_rsi, get_slope
from indicadores.volumen import calcular_volumen_alto
from typing import Tuple


def rsi_bajo(df: pd.DataFrame, limite: float=30.0) ->Tuple[bool, float | None]:
    rsi = get_rsi(df)
    if rsi is None:
        return False, None
    return rsi < limite, float(rsi)


def rsi_cruce_descendente(df: pd.DataFrame, umbral: float=70.0) ->Tuple[
    bool, float | None]:
    serie = get_rsi(df, serie_completa=True)
    if serie is None or len(serie.dropna()) < 2:
        return False, None
    valido = serie.iloc[-2] > umbral and serie.iloc[-1] < umbral
    return valido, float(serie.iloc[-1])


def volumen_suficiente(df: pd.DataFrame, factor: float=1.5, ventana: int=20
    ) ->bool:
    return calcular_volumen_alto(df, factor=factor, ventana=ventana)


def slope_favorable(df: pd.DataFrame, tendencia: (str | None)=None, umbral:
    float=0.0) ->Tuple[bool, float | None]:
    pendiente = get_slope(df)
    if pendiente is None:
        return False, None
    if tendencia == 'alcista':
        valido = pendiente > umbral
    elif tendencia == 'bajista':
        valido = pendiente < -umbral
    else:
        valido = abs(pendiente) > umbral
    return valido, float(pendiente)
