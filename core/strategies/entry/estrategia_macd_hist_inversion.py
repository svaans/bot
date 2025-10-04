import pandas as pd
from indicadores.macd import calcular_macd


def estrategia_macd_hist_inversion(df: pd.DataFrame) ->dict:
    if len(df) < 35:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    _, _, hist_anterior = calcular_macd(df.iloc[:-1])
    _, _, hist_actual = calcular_macd(df)
    if hist_anterior is None or hist_actual is None:
        return {'activo': False, 'mensaje': 'Histograma MACD no disponible'}
    if len(hist_anterior) == 0 or len(hist_actual) == 0:
        return {'activo': False, 'mensaje': 'Histograma MACD vacío'}
    if hist_anterior.iloc[-1] < 0 and hist_actual.iloc[-1] > 0:
        return {'activo': True, 'mensaje':
            'Inversión del histograma MACD detectada'}
    return {'activo': False, 'mensaje': 'Sin inversión MACD'}
