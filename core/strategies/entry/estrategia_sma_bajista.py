import pandas as pd
from indicadores.sma_bajista import calcular_cruce_sma_bajista


def estrategia_sma_bajista(df: pd.DataFrame) ->dict:
    if len(df) < 25:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce = calcular_cruce_sma_bajista(df)
    if cruce:
        return {'activo': True, 'mensaje': 'Cruce SMA bajista detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce SMA bajista'}
