"""Detección de tendencia del mercado y evaluación de señales persistentes."""
import pandas as pd
from typing import Sequence
from indicators.helpers import get_rsi
from indicators.adx import calcular_adx
from core.strategies.entry.gestor_entradas import evaluar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from core.utils.utils import configurar_logger
log = configurar_logger("tendencia")


def detectar_tendencia(symbol: str, df: pd.DataFrame) -> tuple[str, dict[str, bool]]:
    log.info("➡️ Entrando en detectar_tendencia()")
    """Evalúa la tendencia del mercado de forma simétrica para alza y baja."""
    if df is None or df.empty or "close" not in df.columns or len(df) < 60:
        log.warning(f"⚠️ Datos insuficientes para detectar tendencia en {symbol}")
        return "lateral", {}
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=30, adjust=False).mean()
    delta = df["ema_fast"].iloc[-1] - df["ema_slow"].iloc[-1]
    slope = (df["ema_slow"].iloc[-1] - df["ema_slow"].iloc[-5]) / 5
    rsi = get_rsi(df)
    close_std = df["close"].std()
    umbral = max(close_std * 0.02, 0.1)
    puntos_alcista = 0
    puntos_bajista = 0
    if delta > umbral * 0.8:
        puntos_alcista += 1
    elif delta < -umbral * 0.8:
        puntos_bajista += 1
    if slope > 0.008:
        puntos_alcista += 1
    elif slope < -0.008:
        puntos_bajista += 1
    if rsi is not None:
        if rsi > 58:
            puntos_alcista += 1
        elif rsi < 42:
            puntos_bajista += 1
    adx = calcular_adx(df)
    if adx > 20:
        puntos_alcista += 1
        puntos_bajista += 1
    if puntos_alcista >= 2 and puntos_alcista > puntos_bajista:
        tendencia = "alcista"
    elif puntos_bajista >= 2 and puntos_bajista > puntos_alcista:
        tendencia = "bajista"
    else:
        tendencia = "lateral"
    estrategias = obtener_estrategias_por_tendencia(tendencia)
    estrategias_activas = (
        {nombre: (True) for nombre in estrategias}
        if isinstance(estrategias, list)
        else estrategias if isinstance(estrategias, dict) else {}
    )
    log.info(
        {
            "evento": "deteccion_tendencia",
            "symbol": symbol,
            "tendencia": tendencia,
            "delta_ema": round(delta, 6),
            "slope_local": round(slope, 6),
            "rsi": round(rsi, 2) if rsi else None,
            "adx": round(adx, 2) if adx else None,
        }
    )
    return tendencia, estrategias_activas


def obtener_parametros_persistencia(
    tendencia: str, volatilidad: float
) -> tuple[float, int]:
    log.info("➡️ Entrando en obtener_parametros_persistencia()")
    """Define los requisitos de persistencia según la tendencia y la volatilidad."""
    if tendencia == "lateral":
        return 0.6, 3
    elif volatilidad > 0.02:
        return 0.4, 1
    elif tendencia in {"alcista", "bajista"} and volatilidad > 0.01:
        return 0.45, 2
    else:
        return 0.5, 2


async def señales_repetidas(
    buffer: Sequence[dict],
    estrategias_func: dict[str, float],
    tendencia_actual: str,
    volatilidad_actual: float,
    ventanas: int = 3,
) -> int:
    log.info("➡️ Entrando en señales_repetidas()")
    """
    Evalúa la cantidad de ventanas recientes con activaciones técnicas consistentes.
    """
    if len(buffer) < ventanas + 30:
        return 0
    peso_minimo, min_estrategias = obtener_parametros_persistencia(
        tendencia_actual, volatilidad_actual
    )
    datos = list(buffer)[-(ventanas + 30) :]
    df = pd.DataFrame(datos)
    peso_max = sum(estrategias_func.values()) or 1.0
    contador = 0
    for i in range(-ventanas, 0):
        try:
            ventana = df.iloc[i - 30 : i]
            if ventana.empty or len(ventana) < 10:
                continue
            symbol = df.iloc[i]["symbol"]
            tendencia, _ = detectar_tendencia(symbol, ventana)
            evaluacion = await evaluar_estrategias(symbol, ventana, tendencia)
            if not evaluacion:
                continue
            estrategias_activas = evaluacion.get("estrategias_activas", {})
            estrategias_validas = [
                nombre
                for nombre, activa in estrategias_activas.items()
                if activa and estrategias_func.get(nombre, 0) >= peso_minimo * peso_max
            ]
            if len(estrategias_validas) >= min_estrategias:
                contador += 1
        except Exception as e:
            log.warning(f"⚠️ Fallo al evaluar repetición de señales en {symbol}: {e}")
            continue
    return contador
