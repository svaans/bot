"""Módulo para adaptar dinámicamente la configuración del bot de trading."""

from __future__ import annotations

import pandas as pd
import numpy as np
from core.logger import configurar_logger
from indicadores.atr import calcular_atr
from indicadores.rsi import calcular_rsi
from indicadores.slope import calcular_slope


log = configurar_logger("adaptador_dinamico")


def _validar_dataframe(df: pd.DataFrame) -> bool:
    columnas = {"open", "high", "low", "close", "volume"}
    return df is not None and columnas.issubset(df.columns) and len(df) >= 30


def adaptar_configuracion(symbol: str, df: pd.DataFrame) -> dict:
    """Ajusta los parámetros del bot según contexto de mercado."""
    if not _validar_dataframe(df):
        log.warning(f"[{symbol}] Datos insuficientes para adaptación dinámica.")
        return {}

    df = df.tail(60).copy()
    close_actual = df["close"].iloc[-1]

    atr = calcular_atr(df)
    rsi = calcular_rsi(df)
    ma30 = df["close"].rolling(30).mean().dropna()
    slope = calcular_slope(pd.DataFrame({"close": ma30})) if not ma30.empty else 0.0

    if atr is None or rsi is None:
        log.warning(f"[{symbol}] Indicadores insuficientes para adaptación.")
        return {}

    atr_pct = atr / close_actual if close_actual else 0.0
    slope_pct = slope / close_actual if close_actual else 0.0

    volumen_actual = float(df["volume"].iloc[-1])
    volumen_prom_30 = float(df["volume"].rolling(30).mean().iloc[-1])
    volumen_relativo = (
        volumen_actual / volumen_prom_30 if volumen_prom_30 else 1.0
    )

    # --- Adaptación de parámetros técnicos previos -----------------------------
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

    modo_agresivo = abs(rsi - 50) > 20 or abs(slope_pct) > 0.002

    factor_umbral = 1.0
    if atr_pct > 0.02:
        factor_umbral += 0.1
    if abs(slope_pct) < 0.0005:
        factor_umbral += 0.1
    factor_umbral = min(factor_umbral, 1.3)

    sl_ratio = 1.5
    tp_ratio = 3.0
    if atr_pct > 0.02:
        sl_ratio *= 1.2
    if slope_pct > 0.001:
        tp_ratio *= 1.1
    elif slope_pct < -0.001:
        tp_ratio *= 0.9

    riesgo_maximo_diario = 2.0
    if atr_pct < 0.01 and abs(slope_pct) > 0.001:
        riesgo_maximo_diario *= 1.2
    elif atr_pct > 0.02:
        riesgo_maximo_diario *= 0.8

    cooldown_tras_perdida = 2
    if atr_pct > 0.02 or abs(slope_pct) < 0.0005:
        cooldown_tras_perdida = 4
    elif atr_pct < 0.01 and abs(slope_pct) > 0.001:
        cooldown_tras_perdida = 1

    diversidad_minima = 2
    if modo_agresivo and atr_pct < 0.015:
        diversidad_minima = 1
    elif not modo_agresivo and atr_pct > 0.02:
        diversidad_minima = 2

    config = {
        "modo_agresivo": modo_agresivo,
        "factor_umbral": round(factor_umbral, 2),
        "tp_ratio": round(tp_ratio, 2),
        "sl_ratio": round(sl_ratio, 2),
        "riesgo_maximo_diario": round(riesgo_maximo_diario, 4),
        "cooldown_tras_perdida": int(cooldown_tras_perdida),
        "diversidad_minima": int(diversidad_minima),
        "min_slope": min_slope,
        "max_rsi": max_rsi,
        "min_volumen_relativo": min_volumen_relativo,
    }

    log.info(
        f"[{symbol}] Config adaptada | ATR%={atr_pct:.4f} | RSI={rsi:.2f} | "
        f"Slope%={slope_pct:.4f} | Aggresivo={modo_agresivo} | "
        f"minSlope={min_slope} | maxRSI={max_rsi} | minVolRel={min_volumen_relativo}"
    )

    return config
