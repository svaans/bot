import numpy as np
import json
import os
import pandas as pd
from core.logger import configurar_logger
from core.contexto_externo import obtener_puntaje_contexto
from scipy.stats import linregress
from ta.momentum import RSIIndicator

log = configurar_logger("umbral")

# === CONFIGURACIÓN GLOBAL ===
RUTA_CONFIGS_OPTIMAS = "config/configuraciones_optimas.json"
if os.path.exists(RUTA_CONFIGS_OPTIMAS):
    with open(RUTA_CONFIGS_OPTIMAS, "r") as f:
        CONFIGS_OPTIMAS = json.load(f)
else:
    log.warning("❌ Archivo de configuración no encontrado. Se usará configuración por defecto.")
    CONFIGS_OPTIMAS = {}

UMBRAL_POR_DEFECTO = 10
MIN_LONGITUD_DATA = 30
PESO_VOLATILIDAD = 0.4
PESO_RANGO = 0.4
PESO_VOLUMEN = 0.2

# === FUNCIONES AUXILIARES ===
def _limites_adaptativos(contexto_score: float) -> tuple[float, float]:
    """Devuelve el máximo y mínimo del umbral de forma dinámica."""
    base_max = 10.0
    base_min = 1.0
    umbral_max = max(5.0, min(30.0, base_max + contexto_score))
    umbral_min = max(0.5, base_min + contexto_score * 0.5)
    if umbral_max < umbral_min:
        umbral_max = umbral_min + 1.0
    return umbral_max, umbral_min

# === FUNCIÓN PRINCIPAL ===
def calcular_umbral_adaptativo(
    symbol,
    df,
    estrategias_activadas,
    pesos_symbol,
    persistencia: float = 0.0,
    config=None,
):
    """Calcula un umbral técnico adaptativo basado en contexto, configuración y persistencia."""
    if df is None or len(df) < 30 or not estrategias_activadas:
        log.warning(f"⚠️ [{symbol}] Datos insuficientes o sin estrategias activas. Umbral: {UMBRAL_POR_DEFECTO}")
        return UMBRAL_POR_DEFECTO

    columnas_necesarias = {"close", "high", "low", "volume"}
    if not columnas_necesarias.issubset(df.columns):
        log.warning(f"❌ [{symbol}] Faltan columnas clave en el DataFrame: {columnas_necesarias}")
        return UMBRAL_POR_DEFECTO

    # === Ventanas ===
    ventana_close = df["close"].tail(10)
    ventana_high = df["high"].tail(10)
    ventana_low = df["low"].tail(10)
    ventana_vol = df["volume"].tail(30)

    # === Métricas de mercado ===
    media_close = np.mean(ventana_close)
    if media_close == 0 or np.isnan(media_close):
        log.info(f"⚠️ [{symbol}] Media de cierre inválida. Contexto neutral.")
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

    # === Carga de configuración ===
    if config:
        ajuste_volatilidad = config.get("ajuste_volatilidad", 1.0)
        factor_umbral = config.get("factor_umbral", 1.0)
        ajuste_riesgo = config.get("riesgo_maximo_diario", 1.0)
    else:
        config_symbol = CONFIGS_OPTIMAS.get(symbol, {})
        ajuste_volatilidad = config_symbol.get("ajuste_volatilidad", 1.0)
        factor_umbral = config_symbol.get("factor_umbral", 1.0)
        ajuste_riesgo = config_symbol.get("riesgo_maximo_diario", 1.0)

    # === Cálculo de contexto ===
    contexto_score = (
        (volatilidad * 0.3 +
         rango_medio * 0.3 +
         volumen_relativo * 0.2 +
         momentum_std * 0.2) * 10
    ) * ajuste_volatilidad

    if 40 < rsi < 60:
        penalizacion = 1 - (1 - abs(rsi - 50) / 10) * 0.25
        contexto_score *= penalizacion

    contexto_score += obtener_puntaje_contexto(symbol)

    # === Potencia técnica ===
    total_puntaje = sum(pesos_symbol.get(k, 0) for k in estrategias_activadas)
    potencia_tecnica = min(total_puntaje / max(len(estrategias_activadas), 1), 20)

    # === Ajustes de riesgo ===
    ajuste_riesgo = min(ajuste_riesgo, 1.3)
    if contexto_score < 4:
        ajuste_riesgo += 0.5
    if potencia_tecnica < 5:
        ajuste_riesgo += 0.5
    if slope < 0:
        ajuste_riesgo += 0.2

    # === Persistencia ===
    factor_persistencia = 1 - min(persistencia * 0.05, 0.2)

    # === Umbral final ===
    max_dinamico, min_dinamico = _limites_adaptativos(contexto_score)
    umbral_base = min(potencia_tecnica * ajuste_riesgo, max_dinamico)
    umbral = max(
        min(umbral_base * factor_umbral * factor_persistencia, max_dinamico),
        min_dinamico,
    )

    # === Logging completo ===
    log.debug(
        f"📊 [{symbol}] Umbral: {umbral:.2f} | Base: {umbral_base:.2f} | "
        f"Limites({min_dinamico:.2f}-{max_dinamico:.2f}) | Contexto: {contexto_score:.2f} | "
        f"Potencia: {potencia_tecnica:.2f} | Slope: {slope:.4f} | RSI: {rsi:.2f} | "
        f"Momentum: {momentum_std:.4f} | VolAdj: {ajuste_volatilidad:.2f} | "
        f"FactorUmbral: {factor_umbral:.2f} | Riesgo: {ajuste_riesgo:.2f} | "
        f"Persistencia: {persistencia:.2f} | FactorPersistencia: {factor_persistencia:.2f}"
    )

    return umbral

# === FUNCIÓN DE TP/SL ADAPTATIVO ===
def calcular_tp_sl_adaptativos(df, precio_actual, config=None, capital_actual=None, symbol: str = "SYM"):
    if config is None:
        config = {}

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
    atr = df["tr"].rolling(window=14).mean().iloc[-1]

    if pd.isna(atr):
        atr = precio_actual * 0.01  # fallback

    multiplicador_sl = config.get("sl_ratio", 1.5)
    multiplicador_tp = config.get("tp_ratio", 2.5)

    if config.get("modo_capital_bajo") and capital_actual is not None and capital_actual < 500:
        factor = 1 + (1 - capital_actual / 500) * 0.2
        multiplicador_tp *= factor
        multiplicador_sl *= max(0.5, 1 - (1 - capital_actual / 500) * 0.1)

    sl = round(precio_actual - atr * multiplicador_sl, 6)
    tp = round(precio_actual + atr * multiplicador_tp, 6)

    log.debug(
        f"[{symbol}] TP/SL adaptativos | Precio: {precio_actual:.2f} | ATR: {atr:.5f} | "
        f"SL: {sl:.2f} | TP: {tp:.2f} | Ratios: SL x{multiplicador_sl}, TP x{multiplicador_tp} | "
        f"Capital: {capital_actual}"
    )

    return sl, tp






