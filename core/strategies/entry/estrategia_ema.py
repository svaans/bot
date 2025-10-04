import pandas as pd
from indicadores.ema import calcular_cruce_ema


def estrategia_ema(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce = calcular_cruce_ema(df)
    if cruce:
        return {'activo': True, 'mensaje': 'Cruce alcista EMA detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce EMA'}
