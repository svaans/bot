import pandas as pd
from indicadores.divergencia_rsi import calcular_divergencia_rsi


def estrategia_divergencia_rsi(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    divergencia = calcular_divergencia_rsi(df)
    if divergencia:
        return {'activo': True, 'mensaje':
            'Divergencia alcista en RSI detectada'}
    return {'activo': False, 'mensaje': 'Sin divergencia RSI'}
