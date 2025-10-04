import pandas as pd
from indicadores.ichimoku import calcular_ichimoku_breakout


def estrategia_ichimoku_breakout(df: pd.DataFrame) ->dict:
    if len(df) < 52:
        return {'activo': False, 'mensaje': 'Datos insuficientes para Ichimoku'
            }
    tenkan, kijun = calcular_ichimoku_breakout(df)
    if tenkan is None or kijun is None:
        return {'activo': False, 'mensaje': 'Error en el cálculo de Ichimoku'}
    if tenkan > kijun:
        return {'activo': True, 'mensaje': 'Breakout Ichimoku alcista'}
    return {'activo': False, 'mensaje': 'Sin señal Ichimoku'}
