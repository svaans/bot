import pandas as pd
from indicadores.volatilidad import calcular_volatility_breakout


def volatility_breakout(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    breakout = calcular_volatility_breakout(df)
    if breakout:
        return {'activo': True, 'mensaje': 'Ruptura de volatilidad detectada'}
    return {'activo': False, 'mensaje':
        'No hay ruptura significativa de volatilidad'}
