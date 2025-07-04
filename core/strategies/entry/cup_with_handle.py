import pandas as pd


def cup_with_handle(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    df = df.tail(30).copy()
    mitad = len(df) // 2
    min1 = df['close'].iloc[:mitad].min()
    min2 = df['close'].iloc[mitad:].min()
    max1 = df['close'].iloc[0]
    max2 = df['close'].iloc[-1]
    if min1 < max1 and min2 < max2 and abs(min1 - min2) < 0.03 * df['close'
        ].mean() and max2 > max1:
        return {'activo': True, 'mensaje': 'Posible Cup with Handle'}
    return {'activo': False, 'mensaje': 'Sin patrón Cup with Handle'}
