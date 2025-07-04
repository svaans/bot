import pandas as pd


def triple_top(df: pd.DataFrame) ->dict:
    """
    Detecta patrón de triple techo: tres máximos similares con soporte plano.
    """
    if len(df) < 40:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    df = df.tail(30)
    highs = df['high'].values
    lows = df['low'].values
    p1, p2, p3 = highs[5], highs[15], highs[25]
    tolerancia = 0.015 * df['high'].mean()
    son_similares = abs(p1 - p2) < tolerancia and abs(p2 - p3
        ) < tolerancia and abs(p1 - p3) < tolerancia
    soporte_estable = abs(min(lows[-5:]) - max(lows[-5:])) < tolerancia
    if son_similares and soporte_estable:
        return {'activo': True, 'mensaje': 'Triple Top detectado'}
    return {'activo': False, 'mensaje': 'Sin Triple Top'}
