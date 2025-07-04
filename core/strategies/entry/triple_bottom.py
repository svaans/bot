import pandas as pd


def triple_bottom(df: pd.DataFrame) ->dict:
    """
    Detecta patrón Triple Bottom: tres mínimos similares con resistencia estable.
    """
    if len(df) < 40:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    df = df.tail(30)
    lows = df['low'].values
    highs = df['high'].values
    p1, p2, p3 = lows[5], lows[15], lows[25]
    tolerancia = 0.015 * df['low'].mean()
    minimos_similares = abs(p1 - p2) < tolerancia and abs(p2 - p3
        ) < tolerancia and abs(p1 - p3) < tolerancia
    resistencia_estable = abs(max(highs[-5:]) - min(highs[-5:])) < tolerancia
    if minimos_similares and resistencia_estable:
        return {'activo': True, 'mensaje': 'Triple Bottom detectado'}
    return {'activo': False, 'mensaje': 'Sin Triple Bottom'}
