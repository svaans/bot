"""Gestor de entradas para modo scalping."""

import pandas as pd
from typing import Dict, Callable
from estrategias_scalping import cruce_ema_rapidas, volumen_anormal, vwap_entry, rsi_inverso

ESTRATEGIAS: Dict[str, Callable[[pd.DataFrame], bool]] = {
    "cruce_ema_rapidas": cruce_ema_rapidas.evaluar,
    "volumen_anormal": volumen_anormal.evaluar,
    "vwap_entry": vwap_entry.evaluar,
    "rsi_inverso": rsi_inverso.evaluar,
}


def evaluar(df: pd.DataFrame) -> Dict[str, bool]:
    resultados = {}
    for nombre, funcion in ESTRATEGIAS.items():
        try:
            resultados[nombre] = funcion(df)
        except Exception:
            resultados[nombre] = False
    return resultados