import pandas as pd


def three_descending_peaks(df: pd.DataFrame) ->dict:
    if len(df) < 25:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    picos = df['high'].rolling(window=3, center=True).max()
    picos = picos.dropna().tail(5)
    if len(picos) >= 3 and picos.iloc[-3] > picos.iloc[-2] > picos.iloc[-1]:
        return {'activo': True, 'mensaje': 'Tres picos descendentes detectados'
            }
    return {'activo': False, 'mensaje': 'Sin patrón de tres picos descendentes'
        }
