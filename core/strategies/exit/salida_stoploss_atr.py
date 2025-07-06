import pandas as pd
from indicators.atr import calcular_atr
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
log = configurar_logger('salida_stoploss_atr')


def salida_stoploss_atr(orden: dict, df: pd.DataFrame) ->dict:
    log.info('➡️ Entrando en salida_stoploss_atr()')
    try:
        if len(df) < 20 or 'close' not in df.columns:
            return resultado_salida('Stop Loss', False, 'Datos insuficientes')
        direccion = orden.get('direccion', 'long')
        precio_actual = df['close'].iloc[-1]
        entrada = orden.get('precio_entrada')
        atr = calcular_atr(df)
        if atr is None or entrada is None:
            return resultado_salida('Stop Loss', False,
                'ATR o entrada no disponibles')
        margen = 1.5 * atr
        if direccion in ['long', 'compra']:
            sl_tecnico = entrada - margen
            if precio_actual <= sl_tecnico:
                return resultado_salida('Stop Loss', True,
                    f'SL-ATR activado (long) a {sl_tecnico:.4f}', logger=log)
        elif direccion == 'venta':
            sl_tecnico = entrada + margen
            if precio_actual >= sl_tecnico:
                return resultado_salida('Stop Loss', True,
                    f'SL-ATR activado (short) a {sl_tecnico:.4f}', logger=log)
        return resultado_salida('Stop Loss', False, 'SL-ATR no alcanzado')
    except Exception as e:
        return resultado_salida('Stop Loss', False, f'Error SL-ATR: {e}')
