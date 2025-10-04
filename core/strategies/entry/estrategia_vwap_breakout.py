import pandas as pd
from indicadores.vwap import calcular_vwap


def estrategia_vwap_breakout(df: pd.DataFrame) ->dict:
    if len(df) < 2:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    vwap = calcular_vwap(df)
    close = df['close'].iloc[-1]
    if vwap is None:
        return {'activo': False, 'mensaje': 'VWAP no disponible'}
    if close > vwap:
        return {'activo': True, 'mensaje':
            f'Breakout sobre VWAP ({close:.2f} > {vwap:.2f})'}
    return {'activo': False, 'mensaje':
        f'Precio por debajo del VWAP ({close:.2f} ≤ {vwap:.2f})'}
