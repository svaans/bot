import pandas as pd
from indicadores.ema_bajista import calcular_cruce_ema_bajista


def cruce_medias_bajista(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce = calcular_cruce_ema_bajista(df)
    if cruce:
        return {'activo': True, 'mensaje': 'Cruce EMA bajista detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce bajista'}
