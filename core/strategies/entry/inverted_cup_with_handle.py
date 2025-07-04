import pandas as pd


def inverted_cup_with_handle(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    mitad = len(df) // 2
    max1 = df['close'].iloc[:mitad].max()
    max2 = df['close'].iloc[mitad:].max()
    min1 = df['close'].iloc[0]
    min2 = df['close'].iloc[-1]
    if max1 > min1 and max2 > min2 and abs(max1 - max2) < 0.03 * df['close'
        ].mean():
        return {'activo': True, 'mensaje': 'Inverted Cup with Handle detectado'
            }
    return {'activo': False, 'mensaje': 'Sin patrón inverted cup'}
