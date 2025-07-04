import pandas as pd
from indicators.atr import calcular_atr
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
log = configurar_logger('salida_takeprofit_atr')


def salida_takeprofit_atr(orden: dict, df: pd.DataFrame) ->dict:
    try:
        if len(df) < 20 or 'close' not in df.columns:
            return resultado_salida('Take Profit', False, 'Datos insuficientes'
                )
        direccion = orden.get('direccion', 'long')
        precio_actual = df['close'].iloc[-1]
        entrada = orden.get('precio_entrada')
        atr = calcular_atr(df)
        if atr is None or entrada is None:
            return resultado_salida('Take Profit', False,
                'ATR o entrada no disponibles')
        margen_tp = 2.5 * atr
        if direccion in ['long', 'compra']:
            tp_tecnico = entrada + margen_tp
            if precio_actual >= tp_tecnico:
                return resultado_salida('Take Profit', True,
                    f'TP-ATR alcanzado (long) a {tp_tecnico:.4f}', logger=log)
        elif direccion == 'venta':
            tp_tecnico = entrada - margen_tp
            if precio_actual <= tp_tecnico:
                return resultado_salida('Take Profit', True,
                    f'TP-ATR alcanzado (short) a {tp_tecnico:.4f}', logger=log)
        return resultado_salida('Take Profit', False, 'TP-ATR no alcanzado')
    except Exception as e:
        return resultado_salida('Take Profit', False, f'Error TP-ATR: {e}')
