import pandas as pd
from indicadores.atr_breakout import calcular_atr_breakout


def estrategia_atr_breakout(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    breakout = calcular_atr_breakout(df)
    if breakout:
        return {'activo': True, 'mensaje': 'Breakout por encima del ATR'}
    return {'activo': False, 'mensaje': 'No hay breakout según ATR'}
