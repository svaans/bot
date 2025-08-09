"""Evaluador de salidas que combina múltiples estrategias.
"""
from __future__ import annotations
from typing import Dict, List
import pandas as pd
from .salida_stoploss import verificar_salida_stoploss
from .salida_trailing_stop import verificar_trailing_stop
from .salida_por_macd import salida_por_macd
from .salida_por_rsi import salida_por_rsi
from .salida_por_tendencia import salida_por_tendencia
from .salida_stoploss_atr import salida_stoploss_atr
from .salida_takeprofit_atr import salida_takeprofit_atr
from .salida_tiempo_maximo import salida_tiempo_maximo
from config.exit_defaults import load_exit_config
PRIORIDAD = {'Stop Loss': 3, 'Trailing Stop': 3, 'Cambio de tendencia': 3,
    'SL-ATR': 2, 'Tiempo máximo': 2, 'RSI bajo': 2, 'Cruce bajista de MACD':
    2, 'Take Profit': 1, 'TP-ATR': 1}


def verificar_take_profit(orden: Dict, df: pd.DataFrame, config: (Dict |
    None)=None) ->Dict:
    """Comprueba si se alcanzó el TP adaptado dinámicamente."""
    if df is None or 'close' not in df.columns:
        return {'cerrar': False, 'razon': 'Datos insuficientes'}
    precio_actual = df['close'].iloc[-1]
    direccion = orden.get('direccion', 'long')
    atr = None
    if df is not None and len(df) >= 20:
        rango = df['high'].tail(20) - df['low'].tail(20)
        atr = rango.mean()
    cfg = load_exit_config(orden.get('symbol', 'SYM'))
    if config:
        cfg.update(config)
    ratio = cfg['tp_ratio']
    if atr is not None:
        tp_dinamico = orden.get('precio_entrada', precio_actual
            ) + atr * ratio if direccion in ('long', 'compra') else orden.get(
            'precio_entrada', precio_actual) - atr * ratio
    else:
        tp_dinamico = orden.get('take_profit', precio_actual)
    tp = orden.get('take_profit', tp_dinamico)
    if direccion in ('long', 'compra') and precio_actual >= tp:
        return {'cerrar': True, 'razon': 'Take Profit'}
    if direccion in ('short', 'venta') and precio_actual <= tp:
        return {'cerrar': True, 'razon': 'Take Profit'}
    return {'cerrar': False, 'razon': 'TP no alcanzado'}


async def evaluar_salida_inteligente(orden: Dict, df: pd.DataFrame, config: (Dict |
    None)=None) ->Dict:
    """Evalúa varias estrategias de salida y decide un cierre unificado."""
    resultados: List[Dict] = []
    cfg = load_exit_config(orden.get('symbol', 'SYM'))
    if config:
        cfg.update(config)
    vol_min = cfg['volumen_minimo_salida']
    spread_max = cfg['max_spread_ratio']
    if 'volume' in df.columns and df['volume'].iloc[-1] < vol_min:
        return {'cerrar': False, 'razones': ['Volumen bajo'], 'estrategias_activas': 0, 'motivo_final': 'Volumen insuficiente'}
    if 'spread' in df.columns:
        spread = df['spread'].iloc[-1] / df['close'].iloc[-1]
        if spread > spread_max:
            return {'cerrar': False, 'razones': ['Spread alto'], 'estrategias_activas': 0, 'motivo_final': 'Spread excesivo'}
    res_sl = await verificar_salida_stoploss(orden, df, config=config)
    if res_sl.get('cerrar'):
        resultados.append({'razon': 'Stop Loss', 'detalle': res_sl.get(
            'motivo')})
    cerrar, motivo = verificar_trailing_stop(orden, df['close'].iloc[-1],
        df, config=config)
    if cerrar:
        resultados.append({'razon': 'Trailing Stop', 'detalle': motivo})
    res_trend = salida_por_tendencia(orden, df)
    if res_trend.get('cerrar'):
        resultados.append({'razon': 'Cambio de tendencia', 'detalle':
            res_trend['razon']})
    res_macd = salida_por_macd(orden, df)
    if res_macd.get('cerrar'):
        resultados.append({'razon': 'Cruce bajista de MACD', 'detalle':
            res_macd['razon']})
    res_rsi = salida_por_rsi(df)
    if res_rsi.get('cerrar'):
        resultados.append({'razon': 'RSI bajo', 'detalle': res_rsi['razon']})
    res_sl_atr = salida_stoploss_atr(orden, df)
    if res_sl_atr.get('cerrar'):
        resultados.append({'razon': 'SL-ATR', 'detalle': res_sl_atr['razon']})
    res_tp_atr = salida_takeprofit_atr(orden, df)
    if res_tp_atr.get('cerrar'):
        resultados.append({'razon': 'TP-ATR', 'detalle': res_tp_atr['razon']})
    res_time = salida_tiempo_maximo(orden, df)
    if res_time.get('cerrar'):
        resultados.append({'razon': 'Tiempo máximo', 'detalle': res_time[
            'razon']})
    res_tp = verificar_take_profit(orden, df, config=config)
    if res_tp.get('cerrar'):
        resultados.append({'razon': 'Take Profit', 'detalle': res_tp['razon']})
    razones = [r['razon'] for r in resultados]
    if len(resultados) >= 2:
        return {'cerrar': True, 'razones': razones, 'estrategias_activas':
            len(resultados), 'motivo_final': 'Cierre por confirmación múltiple'
            }
    if resultados:
        razon = resultados[0]['razon']
        prioridad = PRIORIDAD.get(razon, 1)
        if prioridad >= 3:
            motivo = resultados[0]['detalle']
            return {'cerrar': True, 'razones': razones,
                'estrategias_activas': 1, 'motivo_final': motivo or razon}
        if prioridad == 2:
            motivo = resultados[0]['detalle']
            return {'cerrar': True, 'razones': razones,
                'estrategias_activas': 1, 'motivo_final': motivo or razon}
        contradiccion = not res_macd.get('cerrar') and not res_rsi.get('cerrar'
            )
        if razon == 'Take Profit' and contradiccion:
            return {'cerrar': False, 'razones': [
                'Take Profit alcanzado pero señales técnicas alcistas'],
                'estrategias_activas': 1, 'motivo_final':
                'Mantener por evaluación técnica'}
        motivo = resultados[0]['detalle']
        return {'cerrar': True, 'razones': razones, 'estrategias_activas': 
            1, 'motivo_final': motivo or razon}
    return {'cerrar': False, 'razones': [], 'estrategias_activas': 0,
        'motivo_final': 'Sin señales de salida'}
