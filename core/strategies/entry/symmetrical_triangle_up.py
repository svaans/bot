import pandas as pd


def symmetrical_triangle_up(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    altos = df['high'].tail(10).values
    bajos = df['low'].tail(10).values
    if altos[0] > altos[-1] and bajos[0] < bajos[-1]:
        return {'activo': True, 'mensaje':
            'Triángulo simétrico alcista detectado'}
    return {'activo': False, 'mensaje': 'Sin triángulo simétrico alcista'}
