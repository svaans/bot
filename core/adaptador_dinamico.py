"""Herramientas para ajustar parámetros técnicos de forma dinámica.

Provee utilidades para adaptar configuraciones base en función de la
volatilidad reciente y para calcular niveles de ``take profit`` y ``stop
loss`` según el régimen de mercado.
"""
from __future__ import annotations
import os
import json
import numpy as np
import pandas as pd
from scipy.stats import linregress
from indicators.retornos_volatilidad import (
    retornos_log,
    retornos_simples,
    verificar_consistencia,
    volatilidad_welford,
)
from core.utils.utils import configurar_logger
from core.market_regime import detectar_regimen
from core.ajustador_riesgo import (
    ajustar_sl_tp_riesgo,
    es_modo_agresivo,
    RIESGO_MAXIMO_DIARIO_BASE,
)
log = configurar_logger('adaptador_dinamico')
RUTA_CONFIGS_OPTIMAS = 'config/configuraciones_optimas.json'
if os.path.exists(RUTA_CONFIGS_OPTIMAS):
    with open(RUTA_CONFIGS_OPTIMAS, 'r') as f:
        CONFIGS_OPTIMAS = json.load(f)
else:
    log.warning(
        '❌ Archivo de configuración no encontrado. Se usará configuración por defecto.'
        )
    CONFIGS_OPTIMAS: dict = {}


def _adaptar_configuracion_base(symbol: str, df: pd.DataFrame, base_config: dict
    ) ->dict:
    """Ajusta ``base_config`` dinámicamente en función del mercado."""
    if df is None or len(df) < 11 or 'close' not in df.columns:
        log.warning(
            f'[{symbol}] ❌ Datos insuficientes para adaptar configuración.')
        return base_config
    config = base_config.copy()
    precios = df['close'].tail(11)
    simples = retornos_simples(precios)
    logs = retornos_log(precios)
    if verificar_consistencia(simples, logs):
        volatilidad = volatilidad_welford(simples.tail(10))
    else:
        volatilidad = 0
    cierre_reciente = df['close'].tail(10)
    try:
        slope = linregress(range(len(cierre_reciente)), cierre_reciente).slope
    except ValueError:
        slope = 0
    config['ajuste_volatilidad'] = round(min(5.0, 1 + volatilidad * 10), 2)
    factor_dinamico = base_config.get('factor_umbral', 1.0) * (1 + volatilidad)
    config['factor_umbral'] = round(min(3.0, max(0.3, factor_dinamico)), 2)
    precio_ref = precios.iloc[-1] if len(precios) else 0.0
    slope_pct = slope / precio_ref if precio_ref else 0.0
    sl_base = base_config.get('sl_ratio', 1.5)
    tp_base = base_config.get('tp_ratio', 3.0)
    riesgo_base = base_config.get('riesgo_maximo_diario', RIESGO_MAXIMO_DIARIO_BASE)
    sl_ratio, tp_ratio, riesgo_diario = ajustar_sl_tp_riesgo(
        volatilidad, slope_pct, riesgo_base, sl_base, tp_base
    )
    if round(riesgo_diario, 4) != round(riesgo_base, 4):
        log.info(
            f"[{symbol}] Riesgo diario ajustado a {riesgo_diario:.4f}"
        )
    config['sl_ratio'] = sl_ratio
    config['tp_ratio'] = tp_ratio
    base_peso = base_config.get('peso_minimo_total', 0.5)
    config['peso_minimo_total'] = round(min(5.0, base_peso * (1 +
        volatilidad * 1.5)), 2)
    base_div = base_config.get('diversidad_minima', 2)
    diversidad = base_div
    if slope > 0.002:
        diversidad = max(1, base_div - 1)
    elif slope < -0.002:
        diversidad = max(base_div, 2)
    cooldown = min(24, max(0, int(volatilidad * 100)))
    modo_agresivo = es_modo_agresivo(volatilidad, slope_pct)
    if modo_agresivo:
        log.info(
            f"[{symbol}] Modo agresivo activado (Vol={volatilidad:.4f}, Slope%={slope_pct:.4f})"
        )
        diversidad = max(1, base_div - 1)
    config['diversidad_minima'] = diversidad
    config['cooldown_tras_perdida'] = cooldown
    config['modo_agresivo'] = modo_agresivo
    config['ponderar_por_diversidad'] = config['diversidad_minima'] <= 2
    base_mult = base_config.get('multiplicador_estrategias_recurrentes', 1.5)
    config['multiplicador_estrategias_recurrentes'] = round(min(3.0,
        base_mult * (1 + volatilidad)), 2)
    config['riesgo_maximo_diario'] = riesgo_diario
    log.info(
        f"[{symbol}] Config adaptada | Volatilidad={volatilidad:.4f} | Slope={slope:.4f} | Factor Umbral={config['factor_umbral']} | SL={config['sl_ratio']} | TP={config['tp_ratio']} | PesoMin={config['peso_minimo_total']} | Diversidad={config['diversidad_minima']} | Cooldown={config['cooldown_tras_perdida']} | Riesgo Diario={config['riesgo_maximo_diario']} | Aggresivo={config['modo_agresivo']}"
        )
    return config


def calcular_tp_sl_adaptativos(symbol: str, df: pd.DataFrame, config: (dict |
    None)=None, capital_actual: (float | None)=None, precio_actual: (float |
    None)=None) ->tuple[float, float]:
    if config is None:
        config = {}
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df debe ser un DataFrame de pandas')
    if precio_actual is None:
        precio_actual = float(df['close'].iloc[-1])
    columnas_requeridas = {'high', 'low', 'close'}
    if not columnas_requeridas.issubset(df.columns):
        log.warning(
            f'[{symbol}] ❌ Columnas insuficientes para TP/SL. Usando margen fijo.'
            )
        margen = precio_actual * 0.01
        return precio_actual - margen, precio_actual + margen
    df = df.ffill().bfill()
    df['hl'] = df['high'] - df['low']
    df['hc'] = abs(df['high'] - df['close'].shift(1))
    df['lc'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['hl', 'hc', 'lc']].max(axis=1)
    regimen = detectar_regimen(df)
    ventana_atr = 7 if regimen == 'lateral' else 14
    atr = df['tr'].rolling(window=ventana_atr).mean().iloc[-1]
    if pd.isna(atr):
        atr = precio_actual * 0.01
    multiplicador_sl = config.get('sl_ratio', 1.5)
    multiplicador_tp = config.get('tp_ratio', 2.5)
    if regimen == 'lateral':
        multiplicador_sl *= 0.8
        multiplicador_tp *= 0.8
    else:
        multiplicador_sl *= 1.2
        multiplicador_tp *= 1.2
    if config.get('modo_capital_bajo'
        ) and capital_actual is not None and capital_actual < 500:
        factor = 1 + (1 - capital_actual / 500) * 0.2
        multiplicador_tp *= factor
        multiplicador_sl *= max(0.5, 1 - (1 - capital_actual / 500) * 0.1)
    sl = round(precio_actual - atr * multiplicador_sl, 6)
    tp = round(precio_actual + atr * multiplicador_tp, 6)
    log.debug(
        f'[{symbol}] TP/SL adaptativos | Regimen: {regimen} | Precio: {precio_actual:.2f} | ATR: {atr:.5f} | SL: {sl:.2f} | TP: {tp:.2f} | Ratios: SL x{multiplicador_sl}, TP x{multiplicador_tp} | Capital: {capital_actual}'
        )
    return sl, tp
