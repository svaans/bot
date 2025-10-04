import pandas as pd
from indicadores.macd import calcular_macd


def estrategia_macd(df: pd.DataFrame) ->dict:
    """
    Detecta cruce alcista del MACD como señal de entrada.
    """
    if len(df) < 35:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    macd_anterior, signal_anterior, _ = calcular_macd(df.iloc[:-1])
    macd_actual, signal_actual, _ = calcular_macd(df)
    if macd_anterior is None or macd_actual is None:
        return {'activo': False, 'mensaje': 'MACD no disponible'}
    if macd_anterior < signal_anterior and macd_actual > signal_actual:
        return {'activo': True, 'mensaje': 'Cruce MACD alcista detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce MACD'}
