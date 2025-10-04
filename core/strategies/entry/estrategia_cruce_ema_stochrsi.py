import pandas as pd
from indicadores.ema_stochrsi import calcular_cruce_ema_stochrsi


def estrategia_cruce_ema_stochrsi(df: pd.DataFrame) ->dict:
    if len(df) < 50:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce_confirmado = calcular_cruce_ema_stochrsi(df)
    if cruce_confirmado:
        return {'activo': True, 'mensaje': 'Cruce EMA + StochRSI alcista'}
    return {'activo': False, 'mensaje': 'Sin cruce EMA + StochRSI'}
