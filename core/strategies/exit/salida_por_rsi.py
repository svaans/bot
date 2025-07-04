import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
log = configurar_logger('salida_por_rsi')


def salida_por_rsi(df: pd.DataFrame, umbral_bajo=30) ->dict:
    """
    Cierra si RSI cae por debajo del umbral (ej: 30)
    """
    if len(df) < 15:
        return resultado_salida('Tecnico', False, 'Insuficientes datos')
    rsi = RSIIndicator(close=df['close'], window=14).rsi()
    if rsi.iloc[-1] < umbral_bajo:
        return resultado_salida('Tecnico', True,
            f'RSI bajo ({rsi.iloc[-1]:.2f}) < {umbral_bajo}', logger=log)
    return resultado_salida('Tecnico', False, 'RSI no bajo')
