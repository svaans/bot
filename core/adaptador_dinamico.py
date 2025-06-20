from __future__ import annotations

import os
import json
import numpy as np
import pandas as pd
from scipy.stats import linregress
from ta.momentum import RSIIndicator
from core.logger import configurar_logger
from core.contexto_externo import obtener_puntaje_contexto
from core.market_regime import detectar_regimen


log = configurar_logger("adaptador_dinamico")

# --- Configuración global para umbrales -------------------------------------
RUTA_CONFIGS_OPTIMAS = "config/configuraciones_optimas.json"
if os.path.exists(RUTA_CONFIGS_OPTIMAS):
    with open(RUTA_CONFIGS_OPTIMAS, "r") as f:
        CONFIGS_OPTIMAS = json.load(f)
else:
    log.warning("❌ Archivo de configuración no encontrado. Se usará configuración por defecto.")
    CONFIGS_OPTIMAS: dict = {}

UMBRAL_POR_DEFECTO = 10
MIN_LONGITUD_DATA = 30
PESO_VOLATILIDAD = 0.4
PESO_RANGO = 0.4
PESO_VOLUMEN = 0.2

# --- Adaptación de configuración -------------------------------------------
def adaptar_configuracion(symbol: str, df: pd.DataFrame, base_config: dict) -> dict:
    """Ajusta ``base_config`` dinámicamente en función del mercado."""
    if df is None or len(df) < 10 or "close" not in df.columns:
        log.warning(f"[{symbol}] ❌ Datos insuficientes para adaptar configuración.")
        return base_config

    config = base_config.copy()
    cambios = df["close"].pct_change().dropna().tail(10)
    volatilidad = cambios.std() if not cambios.empty else 0

    cierre_reciente = df["close"].tail(10)
    try:
        slope = linregress(range(len(cierre_reciente)), cierre_reciente).slope
    except ValueError:
        slope = 0

    config["ajuste_volatilidad"] = round(min(5.0, 1 + volatilidad * 10), 2)
    factor_dinamico = base_config.get("factor_umbral", 1.0) * (1 + volatilidad)
    config["factor_umbral"] = round(min(3.0, max(0.3, factor_dinamico)), 2)

    sl_ratio = base_config.get("sl_ratio", 2.0)
    tp_ratio = base_config.get("tp_ratio", 4.0)
    if slope < -0.001:
        sl_ratio *= 1.1
        tp_ratio *= 0.9
    elif slope > 0.001:
        sl_ratio *= 0.9
        tp_ratio *= 1.1
    config["sl_ratio"] = round(min(5.0, max(sl_ratio, 0.5)), 2)
    config["tp_ratio"] = round(min(8.0, max(tp_ratio, 1.0)), 2)

    base_peso = base_config.get("peso_minimo_total", 2.0)
    config["peso_minimo_total"] = round(min(5.0, base_peso * (1 + volatilidad * 1.5)), 2)

    if slope < -0.002:
        config["diversidad_minima"] = 2
    elif slope > 0.002:
        config["diversidad_minima"] = 1
    else:
        config["diversidad_minima"] = 2

    cooldown = min(24, max(0, int(volatilidad * 100)))
    config["cooldown_tras_perdida"] = cooldown
    config["modo_agresivo"] = volatilidad > 0.01 or slope > 0.003
    config["ponderar_por_diversidad"] = config["diversidad_minima"] <= 2

    base_mult = base_config.get("multiplicador_estrategias_recurrentes", 1.5)
    config["multiplicador_estrategias_recurrentes"] = round(min(3.0, base_mult * (1 + volatilidad)), 2)

    base_riesgo = base_config.get("riesgo_maximo_diario", 5.0)
    config["riesgo_maximo_diario"] = round(min(10.0, base_riesgo + volatilidad * 5), 2)

    log.info(
        f"[{symbol}] Config adaptada | Volatilidad={volatilidad:.4f} | Slope={slope:.4f} | "
        f"Factor Umbral={config['factor_umbral']} | SL={config['sl_ratio']} | TP={config['tp_ratio']} | "
        f"PesoMin={config['peso_minimo_total']} | Diversidad={config['diversidad_minima']} | "
        f"Cooldown={config['cooldown_tras_perdida']} | Riesgo Diario={config['riesgo_maximo_diario']} | "
        f"Aggresivo={config['modo_agresivo']}"
    )
    return config


# --- Umbral adaptativo ------------------------------------------------------

def _limites_adaptativos(contexto_score: float) -> tuple[float, float]:
    base_max = 10.0
    base_min = 1.0
    umbral_max = max(5.0, min(30.0, base_max + contexto_score))
    umbral_min = max(0.5, base_min + contexto_score * 0.5)
    if umbral_max < umbral_min:
        umbral_max = umbral_min + 1.0
    return umbral_max, umbral_min



def calcular_umbral_adaptativo(
    symbol: str,
    df: pd.DataFrame,
    estrategias_activadas,
    pesos_symbol,
    persistencia: float = 0.0,
    config: dict | None = None,
) -> float:
    """Calcula un umbral técnico adaptativo."""
    if df is None or len(df) < MIN_LONGITUD_DATA or not estrategias_activadas:
        log.warning(f"⚠️ [{symbol}] Datos insuficientes o sin estrategias activas. Umbral: {UMBRAL_POR_DEFECTO}")
        return UMBRAL_POR_DEFECTO

    columnas_necesarias = {"close", "high", "low", "volume"}
    if not columnas_necesarias.issubset(df.columns):
        log.warning(f"❌ [{symbol}] Faltan columnas clave en el DataFrame: {columnas_necesarias}")
        return UMBRAL_POR_DEFECTO

    
    ventana_close = df["close"].tail(10)
    ventana_high = df["high"].tail(10)
    ventana_low = df["low"].tail(10)
    ventana_vol = df["volume"].tail(30)

    
    media_close = np.mean(ventana_close)
    if media_close == 0 or np.isnan(media_close):
        volatilidad = 0
        rango_medio = 0
    else:
        volatilidad = np.std(ventana_close) / media_close
        rango_medio = np.mean(ventana_high - ventana_low) / media_close

    volumen_promedio = ventana_vol.mean()
    volumen_max = ventana_vol.max()
    volumen_relativo = 0.5 if volumen_max == 0 or np.isnan(volumen_max) else volumen_promedio / volumen_max

    momentum_std = df["close"].pct_change().tail(5).std()

    try:
        slope = linregress(range(len(ventana_close)), ventana_close).slope
    except ValueError as e:
        log.warning(f"⚠️ Error calculando slope para {symbol}: {e}")
        slope = 0

    try:
        rsi = RSIIndicator(close=df["close"], window=14).rsi().iloc[-1]
    except Exception as e:
        log.warning(f"⚠️ Error calculando RSI para {symbol}: {e}")
        rsi = 50

    
    if config:
        ajuste_volatilidad = config.get("ajuste_volatilidad", 1.0)
        factor_umbral = config.get("factor_umbral", 1.0)
        ajuste_riesgo = config.get("riesgo_maximo_diario", 1.0)
    else:
        cfg_sym = CONFIGS_OPTIMAS.get(symbol, {})
        ajuste_volatilidad = cfg_sym.get("ajuste_volatilidad", 1.0)
        factor_umbral = cfg_sym.get("factor_umbral", 1.0)
        ajuste_riesgo = cfg_sym.get("riesgo_maximo_diario", 1.0)

    
    contexto_score = (
        (volatilidad * 0.3 + rango_medio * 0.3 + volumen_relativo * 0.2 + momentum_std * 0.2) * 10
    ) * ajuste_volatilidad

    if 40 < rsi < 60:
        penalizacion = 1 - (1 - abs(rsi - 50) / 10) * 0.25
        contexto_score *= penalizacion

    contexto_extra = obtener_puntaje_contexto(symbol)
    try:
        contexto_score += float(contexto_extra)
    except (TypeError, ValueError):
        log.warning(f"[{symbol}] Puntaje de contexto inválido: {contexto_extra}")

    
    pesos_validos = [pesos_symbol.get(k, 0) for k in estrategias_activadas if pesos_symbol.get(k, 0) > 0]
    if pesos_validos:
        total_puntaje = sum(pesos_validos)
        if max(pesos_validos) >= 8:
            potencia_tecnica = min(sum(np.exp(p / 10) for p in pesos_validos), 30)
        else:
            potencia_tecnica = min(total_puntaje, 30)
    else:
        potencia_tecnica = 0.0

    
    ajuste_riesgo = min(ajuste_riesgo, 1.3)
    if contexto_score < 4:
        ajuste_riesgo += 0.5
    if potencia_tecnica < 5:
        ajuste_riesgo += 0.5
    if slope < 0:
        ajuste_riesgo += 0.2

    
    dinamica_persistencia = 0.05 + min(abs(slope) * 0.1, 0.15) + min(momentum_std * 2, 0.1)
    factor_persistencia = 1 - min(persistencia * dinamica_persistencia, 0.3)

    
    max_dinamico, min_dinamico = _limites_adaptativos(contexto_score)
    umbral_base = min(potencia_tecnica * ajuste_riesgo, max_dinamico)
    umbral = max(
        min(umbral_base * factor_umbral * factor_persistencia, max_dinamico),
        min_dinamico,
    )

    
    log.debug(
        f"📊 [{symbol}] Umbral: {umbral:.2f} | Base: {umbral_base:.2f} | "
        f"Limites({min_dinamico:.2f}-{max_dinamico:.2f}) | Contexto: {contexto_score:.2f} | "
        f"Potencia: {potencia_tecnica:.2f} | Slope: {slope:.4f} | RSI: {rsi:.2f} | "
        f"Momentum: {momentum_std:.4f} | VolAdj: {ajuste_volatilidad:.2f} | "
        f"FactorUmbral: {factor_umbral:.2f} | Riesgo: {ajuste_riesgo:.2f} | "
        f"Persistencia: {persistencia:.2f} | FactorPersistencia: {factor_persistencia:.2f}"
    )
    return umbral

# --- TP/SL adaptativos ------------------------------------------------------

def calcular_tp_sl_adaptativos(
    symbol: str,
    df: pd.DataFrame,
    config: dict | None = None,
    capital_actual: float | None = None,
    precio_actual: float | None = None,
) -> tuple[float, float]:
    if config is None:
        config = {}
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser un DataFrame de pandas")
    if precio_actual is None:
        precio_actual = float(df["close"].iloc[-1])

    columnas_requeridas = {"high", "low", "close"}
    if not columnas_requeridas.issubset(df.columns):
        log.warning(f"[{symbol}] ❌ Columnas insuficientes para TP/SL. Usando margen fijo.")
        margen = precio_actual * 0.01
        return precio_actual - margen, precio_actual + margen

    df = df.ffill().bfill()
    df["hl"] = df["high"] - df["low"]
    df["hc"] = abs(df["high"] - df["close"].shift(1))
    df["lc"] = abs(df["low"] - df["close"].shift(1))
    df["tr"] = df[["hl", "hc", "lc"]].max(axis=1)
    regimen = detectar_regimen(df)
    ventana_atr = 7 if regimen == "lateral" else 14
    atr = df["tr"].rolling(window=ventana_atr).mean().iloc[-1]
    if pd.isna(atr):
        atr = precio_actual * 0.01

    multiplicador_sl = config.get("sl_ratio", 1.5)
    multiplicador_tp = config.get("tp_ratio", 2.5)
    if regimen == "lateral":
        multiplicador_sl *= 0.8
        multiplicador_tp *= 0.8
    else:
        multiplicador_sl *= 1.2
        multiplicador_tp *= 1.2

    if config.get("modo_capital_bajo") and capital_actual is not None and capital_actual < 500:
        factor = 1 + (1 - capital_actual / 500) * 0.2
        multiplicador_tp *= factor
        multiplicador_sl *= max(0.5, 1 - (1 - capital_actual / 500) * 0.1)

    sl = round(precio_actual - atr * multiplicador_sl, 6)
    tp = round(precio_actual + atr * multiplicador_tp, 6)

    log.debug(
        f"[{symbol}] TP/SL adaptativos | Regimen: {regimen} | Precio: {precio_actual:.2f} | ATR: {atr:.5f} | "
        f"SL: {sl:.2f} | TP: {tp:.2f} | Ratios: SL x{multiplicador_sl}, TP x{multiplicador_tp} | "
        f"Capital: {capital_actual}"
    )
    return sl, tp



