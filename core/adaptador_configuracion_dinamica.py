"""Módulo para adaptar dinámicamente la configuración del bot de trading."""
from __future__ import annotations
import pandas as pd
import numpy as np
from core.utils.utils import configurar_logger
from indicators.helpers import get_atr, get_rsi, get_slope
from core.ajustador_riesgo import ajustar_sl_tp_riesgo
log = configurar_logger('adaptador_dinamico')


def _validar_dataframe(df: pd.DataFrame) ->bool:
    log.info('➡️ Entrando en _validar_dataframe()')
    columnas = {'open', 'high', 'low', 'close', 'volume'}
    return df is not None and columnas.issubset(df.columns) and len(df) >= 30


def _validar_coherencia_tp_sl(sl: float, tp: float, ratio_min: float=1.1
    ) ->bool:
    log.info('➡️ Entrando en _validar_coherencia_tp_sl()')
    """Comprueba que los ratios TP/SL sean coherentes."""
    if sl <= 0 or tp <= 0:
        return False
    if tp <= sl * ratio_min:
        return False
    if tp / sl > 10:
        return False
    return True


def _alertar_inconsistencias(symbol: str, sl: float, tp: float) ->None:
    log.info('➡️ Entrando en _alertar_inconsistencias()')
    """Emite alertas cuando la configuración es incoherente."""
    if not _validar_coherencia_tp_sl(sl, tp):
        log.warning(
            f'[{symbol}] Inconsistencia detectada en ratios TP/SL: TP={tp}, SL={sl}'
            )


def _adaptar_configuracion_indicadores(symbol: str, df: pd.DataFrame, base_config: dict | None=None) ->dict:
    log.info('➡️ Entrando en _adaptar_configuracion_indicadores()')
    """Ajusta los parámetros del bot según contexto de mercado."""
    if not _validar_dataframe(df):
        log.warning(f'[{symbol}] Datos insuficientes para adaptación dinámica.')
        return {}
    base_config = base_config or {}
    df = df.tail(60).copy()
    close_actual = df['close'].iloc[-1]
    atr = get_atr(df)
    rsi = get_rsi(df)
    ma30 = df['close'].rolling(30).mean().dropna()
    slope = get_slope(pd.DataFrame({'close': ma30})
        ) if not ma30.empty else 0.0
    if atr is None or rsi is None:
        log.warning(f'[{symbol}] Indicadores insuficientes para adaptación.')
        return {}
    atr_pct = atr / close_actual if close_actual else 0.0
    if atr_pct > 0.05:
        log.warning(f'[{symbol}] Volatilidad extrema detectada: {atr_pct:.4f}')
        atr_pct = 0.05
    atr_pct = max(0.0, atr_pct)
    slope_pct = slope / close_actual if close_actual else 0.0
    volumen_actual = float(df['volume'].iloc[-1])
    volumen_prom_30 = float(df['volume'].rolling(30).mean().iloc[-1])
    volumen_relativo = (volumen_actual / volumen_prom_30 if volumen_prom_30
         else 1.0)
    if atr_pct <= 0.008:
        min_slope = 0.01
    elif atr_pct >= 0.015:
        min_slope = 0.03
    else:
        t = np.clip((atr_pct - 0.008) / (0.015 - 0.008), 0.0, 1.0)
        min_slope = 0.01 + t * (0.03 - 0.01)
    min_slope = round(float(min_slope), 3)
    if rsi < 50:
        max_rsi = 70.0
    elif rsi > 60:
        max_rsi = 65.0
    else:
        max_rsi = 68.0
    max_rsi = round(float(max_rsi), 3)
    if volumen_relativo <= 1.2:
        min_volumen_relativo = 1.1
    elif volumen_relativo >= 1.5:
        min_volumen_relativo = 1.3
    else:
        t = np.clip((volumen_relativo - 1.2) / (1.5 - 1.2), 0.0, 1.0)
        min_volumen_relativo = 1.1 + t * (1.3 - 1.1)
    min_volumen_relativo = round(float(min_volumen_relativo), 3)
    modo_agresivo = atr_pct > 0.02 or abs(rsi - 50) > 20 or abs(slope_pct
        ) > 0.002
    factor_umbral = 1.0
    if atr_pct > 0.02:
        factor_umbral += 0.1
    if abs(slope_pct) < 0.0005:
        factor_umbral += 0.1
    factor_umbral = min(factor_umbral, 1.3)
    sl_base = base_config.get('sl_ratio', 1.5)
    tp_base = base_config.get('tp_ratio', 3.0)
    riesgo_base = base_config.get('riesgo_maximo_diario', 2.0)
    sl_ratio, tp_ratio, riesgo_maximo_diario = ajustar_sl_tp_riesgo(
        atr_pct, slope_pct, riesgo_base, sl_base, tp_base
    )
    if not _validar_coherencia_tp_sl(sl_ratio, tp_ratio):
        _alertar_inconsistencias(symbol, sl_ratio, tp_ratio)
    cooldown_tras_perdida = 2
    if atr_pct > 0.02 or abs(slope_pct) < 0.0005:
        cooldown_tras_perdida = 4
    elif atr_pct < 0.01 and abs(slope_pct) > 0.001:
        cooldown_tras_perdida = 1
    diversidad_minima = base_config.get('diversidad_minima', 2)
    if modo_agresivo or atr_pct < 0.012 or slope_pct > 0.003:
        diversidad_minima = max(1, diversidad_minima - 1)
    config = {'modo_agresivo': modo_agresivo, 'factor_umbral': round(
        factor_umbral, 2), 'tp_ratio': round(tp_ratio, 2), 'sl_ratio':
        round(sl_ratio, 2), 'riesgo_maximo_diario': round(
        riesgo_maximo_diario, 4), 'cooldown_tras_perdida': int(
        cooldown_tras_perdida), 'diversidad_minima': int(diversidad_minima),
        'min_slope': min_slope, 'max_rsi': max_rsi, 'min_volumen_relativo':
        min_volumen_relativo}
    log.info(
        f'[{symbol}] Config adaptada | ATR%={atr_pct:.4f} | RSI={rsi:.2f} | Slope%={slope_pct:.4f} | Aggresivo={modo_agresivo} | minSlope={min_slope} | maxRSI={max_rsi} | minVolRel={min_volumen_relativo}'
        )
    return config
