import pandas as pd
from .validaciones_comunes import rsi_bajo


def estrategia_rsi(df: pd.DataFrame) ->dict:
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    valido, rsi = rsi_bajo(df, limite=30)
    if rsi is None:
        return {'activo': False, 'mensaje': 'RSI no disponible'}
    if valido:
        return {'activo': True, 'mensaje':
            f'RSI bajo ({rsi:.2f}) → posible rebote'}
    return {'activo': False, 'mensaje': f'RSI actual: {rsi:.2f}'}
