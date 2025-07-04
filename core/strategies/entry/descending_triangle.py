import pandas as pd


def descending_triangle(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    ultimos = df.tail(10)
    altos = ultimos['high']
    bajos = ultimos['low']
    soporte = min(bajos)
    maximos = altos.rolling(window=3).max()
    if maximos.is_monotonic_decreasing:
        return {'activo': True, 'mensaje': 'Triángulo descendente detectado'}
    return {'activo': False, 'mensaje': 'Sin triángulo descendente'}
