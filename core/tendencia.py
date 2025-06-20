"""Detección de tendencia del mercado y evaluación de señales persistentes."""

import pandas as pd

from indicadores.rsi import calcular_rsi
from indicadores.adx import calcular_adx
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from core.logger import configurar_logger

log = configurar_logger("tendencia")


# -------------------- DETECCIÓN DE TENDENCIA --------------------

def detectar_tendencia(symbol: str, df: pd.DataFrame) -> tuple[str, dict[str, bool]]:
    """Detección precisa de tendencia con EMA, pendiente local y RSI."""

    if df is None or df.empty or "close" not in df.columns or len(df) < 60:
        log.warning(f"⚠️ Datos insuficientes para detectar tendencia en {symbol}")
        return "lateral", {}

    df = df.copy()

    # Cálculo de EMAs
    df["ema_fast"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=30, adjust=False).mean()

    # Diferencia entre EMAs (delta)
    delta = df["ema_fast"].iloc[-1] - df["ema_slow"].iloc[-1]

    # Pendiente local de la EMA lenta (últimos 5 puntos)
    slope = (df["ema_slow"].iloc[-1] - df["ema_slow"].iloc[-5]) / 5

    # RSI como indicador adicional
    rsi = calcular_rsi(df)

    # Umbral dinámico adaptado a la volatilidad
    close_std = df["close"].std()
    umbral = max(close_std * 0.02, 0.1)

    # Clasificación de tendencia con sistema de puntos
    puntos = 0
    if delta > umbral * 0.8:
        puntos += 1
    if slope > 0.008:
        puntos += 1
    if rsi is not None and rsi > 58:
        puntos += 1

    # Puedes descomentar esto si decides usar ADX
    adx = calcular_adx(df)
    if adx > 20:
        puntos += 1

    # Decisión final basada en puntos
    if puntos >= 2:
        tendencia = "alcista"
    elif puntos == 1:
        tendencia = "bajista"
    else:
        tendencia = "lateral"

    estrategias = obtener_estrategias_por_tendencia(tendencia)
    estrategias_activas = (
        {nombre: True for nombre in estrategias}
        if isinstance(estrategias, list)
        else (estrategias if isinstance(estrategias, dict) else {})
    )

    log.info({
        "evento": "deteccion_tendencia",
        "symbol": symbol,
        "tendencia": tendencia,
        "delta_ema": round(delta, 6),
        "slope_local": round(slope, 6),
        "rsi": round(rsi, 2) if rsi else None,
        "adx": round(adx, 2) if adx else None  # si activas ADX
    })

    return tendencia, estrategias_activas


# -------------------- AJUSTE DE PERSISTENCIA --------------------

def obtener_parametros_persistencia(tendencia: str, volatilidad: float) -> tuple[float, int]:
    """Define los requisitos de persistencia según la tendencia y la volatilidad."""
    if tendencia == "lateral":
        return 0.6, 3
    elif volatilidad > 0.02:
        return 0.4, 1
    elif tendencia in {"alcista", "bajista"} and volatilidad > 0.01:
        return 0.45, 2
    else:
        return 0.5, 2


# -------------------- DETECCIÓN DE SEÑALES REPETIDAS --------------------

def señales_repetidas(
    buffer: list[dict],
    estrategias_func: dict[str, float],
    tendencia_actual: str,
    volatilidad_actual: float,
    ventanas: int = 3,
) -> int:
    """
    Evalúa la cantidad de ventanas recientes con activaciones técnicas consistentes.
    """
    if len(buffer) < ventanas + 30:
        return 0

    peso_minimo, min_estrategias = obtener_parametros_persistencia(tendencia_actual, volatilidad_actual)
    df = pd.DataFrame(buffer[-(ventanas + 30):])
    peso_max = sum(estrategias_func.values()) or 1.0
    contador = 0

    for i in range(-ventanas, 0):
        try:
            ventana = df.iloc[i - 30:i]
            if ventana.empty or len(ventana) < 10:
                continue

            symbol = df.iloc[i]["symbol"]
            tendencia, _ = detectar_tendencia(symbol, ventana)
            evaluacion = evaluar_estrategias(symbol, ventana, tendencia)
            if not evaluacion:
                continue

            estrategias_activas = evaluacion.get("estrategias_activas", {})
            estrategias_validas = [
                nombre for nombre, activa in estrategias_activas.items()
                if activa and estrategias_func.get(nombre, 0) >= peso_minimo * peso_max
            ]

            if len(estrategias_validas) >= min_estrategias:
                contador += 1

        except Exception as e:
            log.warning(f"⚠️ Fallo al evaluar repetición de señales en {symbol}: {e}")
            continue

    return contador

