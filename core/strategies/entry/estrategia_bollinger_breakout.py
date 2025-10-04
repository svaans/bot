import pandas as pd
from indicadores.bollinger import calcular_bollinger


def estrategia_bollinger_breakout(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    banda_inferior, banda_superior, close = calcular_bollinger(df)
    if banda_inferior is None or banda_superior is None:
        return {'activo': False, 'mensaje': 'Bandas no disponibles'}
    if close > banda_superior:
        return {'activo': True, 'mensaje': 'Breakout superior de Bollinger'}
    elif close < banda_inferior:
        return {'activo': True, 'mensaje': 'Breakout inferior de Bollinger'}
    return {'activo': False, 'mensaje': 'Dentro de bandas de Bollinger'}
