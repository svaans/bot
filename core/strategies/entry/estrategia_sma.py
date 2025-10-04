import pandas as pd
from indicadores.sma import calcular_cruce_sma


def estrategia_sma(df: pd.DataFrame) ->dict:
    if len(df) < 50:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce = calcular_cruce_sma(df)
    if cruce:
        return {'activo': True, 'mensaje':
            'SMA 20 > SMA 50 (tendencia alcista)'}
    return {'activo': False, 'mensaje': 'SMA 20 < SMA 50'}
