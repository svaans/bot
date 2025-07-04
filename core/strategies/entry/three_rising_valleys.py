import pandas as pd


def three_rising_valleys(df: pd.DataFrame) ->dict:
    if len(df) < 25:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    valles = df['low'].rolling(window=3, center=True).min()
    valles = valles.dropna().tail(5)
    if len(valles) >= 3 and valles.iloc[-3] < valles.iloc[-2] < valles.iloc[-1
        ]:
        return {'activo': True, 'mensaje': 'Tres valles ascendentes detectados'
            }
    return {'activo': False, 'mensaje': 'Sin patrón de tres valles ascendentes'
        }
