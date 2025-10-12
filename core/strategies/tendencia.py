"""Detección de tendencia del mercado y evaluación de señales persistentes."""
from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Sequence

import pandas as pd
from indicadores.helpers import get_rsi
from indicadores.adx import calcular_adx
from core.strategies.entry.gestor_entradas import evaluar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from core.utils.utils import configurar_logger
from core.data.bootstrap import (
    MIN_BARS,
    enqueue_fetch,
    was_warned,
    mark_warned,
    update_progress,
)
log = configurar_logger("tendencia")


@dataclass
class _TendenciaState:
    """Mantiene en caché la última evaluación por símbolo."""

    data_hash: tuple[int, int, float]
    delta: float
    slope: float
    rsi: float | None
    adx: float | None
    puntos_alcista: int
    puntos_bajista: int
    tendencia: str


_ESTADO_TENDENCIA: dict[str, _TendenciaState] = {}
_CACHE_METRICAS: dict[str, tuple[tuple[int, int, float], _TendenciaState]] = {}


def _hash_dataframe(df: pd.DataFrame) -> tuple[int, int, float]:
    """Genera una huella barata para detectar cambios relevantes en ``df``."""

    largo = len(df)
    if largo == 0:
        return (0, 0, 0.0)
    try:
        ultimo_ts = int(df["timestamp"].iloc[-1])
    except Exception:
        ultimo_ts = largo
    try:
        ultimo_close = float(df["close"].iloc[-1])
    except Exception:
        ultimo_close = 0.0
    return (largo, ultimo_ts, ultimo_close)


def _almost_equal(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return math.isclose(a, b, rel_tol=1e-3, abs_tol=1e-5)


def _obtener_columna_o_calcular(
    df: pd.DataFrame, nombre: str, span: int
) -> pd.Series:
    """Reutiliza ``nombre`` si existe o calcula una EMA nueva."""

    if nombre in df.columns and len(df[nombre]) == len(df):
        serie = df[nombre]
        if serie.notna().sum() >= max(3, span // 2):
            return serie
    serie = df["close"].ewm(span=span, adjust=False).mean()
    try:
        df.loc[:, nombre] = serie
    except Exception:
        # Slices derivados de ``iloc`` pueden lanzar SettingWithCopy; se ignora.
        pass
    return serie


def _should_emit_log(
    anterior: _TendenciaState | None, nuevo: _TendenciaState
) -> bool:
    if anterior is None:
        return True
    if anterior.tendencia != nuevo.tendencia:
        return True
    if not _almost_equal(anterior.delta, nuevo.delta):
        return True
    if not _almost_equal(anterior.slope, nuevo.slope):
        return True
    if not _almost_equal(anterior.rsi, nuevo.rsi):
        return True
    if not _almost_equal(anterior.adx, nuevo.adx):
        return True
    return False


def _calcular_tendencia_base(
    df: pd.DataFrame,
) -> tuple[float, float, float | None, float | None, int, int]:
    """Calcula métricas básicas de tendencia a partir de ``df``."""
    ema_fast = _obtener_columna_o_calcular(df, "ema_fast", 10)
    ema_slow = _obtener_columna_o_calcular(df, "ema_slow", 30)
    delta = ema_fast.iloc[-1] - ema_slow.iloc[-1]
    slope_raw = (ema_slow.iloc[-1] - ema_slow.iloc[-5]) / 5
    price = df["close"].iloc[-1]
    slope = slope_raw / price if price else 0.0
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
    if adx is not None and adx > 20:
        if delta > 0 and slope > 0:
            puntos_alcista += 1
        elif delta < 0 and slope < 0:
            puntos_bajista += 1

    return delta, slope, rsi, adx, puntos_alcista, puntos_bajista


def detectar_tendencia(
    symbol: str, df: pd.DataFrame, *, registrar: bool = True
) -> tuple[str, dict[str, bool]]:
    """Evalúa la tendencia del mercado de forma simétrica para alza y baja."""
    if df is None or df.empty or "close" not in df.columns or len(df) < 30:
        log.warning(
            f"⚠️ Datos insuficientes para detectar tendencia en {symbol}"
        )
        return "lateral", {}
    symbol_key = str(symbol or "").upper()
    df_hash = _hash_dataframe(df)
    cache_entry = _CACHE_METRICAS.get(symbol_key)
    if cache_entry and cache_entry[0] == df_hash:
        nuevo_estado = cache_entry[1]
    else:
        (
            delta,
            slope,
            rsi,
            adx,
            puntos_alcista,
            puntos_bajista,
        ) = _calcular_tendencia_base(df)
        if puntos_alcista >= 2 and puntos_alcista > puntos_bajista:
            tendencia = "alcista"
        elif puntos_bajista >= 2 and puntos_bajista > puntos_alcista:
            tendencia = "bajista"
        else:
            tendencia = "lateral"
        nuevo_estado = _TendenciaState(
            data_hash=df_hash,
            delta=float(delta),
            slope=float(slope),
            rsi=None if rsi is None else float(rsi),
            adx=None if adx is None else float(adx),
            puntos_alcista=int(puntos_alcista),
            puntos_bajista=int(puntos_bajista),
            tendencia=tendencia,
        )
        if registrar:
            _CACHE_METRICAS[symbol_key] = (df_hash, nuevo_estado)
    delta = nuevo_estado.delta
    slope = nuevo_estado.slope
    rsi = nuevo_estado.rsi
    adx = nuevo_estado.adx
    puntos_alcista = nuevo_estado.puntos_alcista
    puntos_bajista = nuevo_estado.puntos_bajista
    tendencia = nuevo_estado.tendencia
    emitir_log = False
    if registrar:
        estado_anterior = _ESTADO_TENDENCIA.get(symbol_key)
        emitir_log = _should_emit_log(estado_anterior, nuevo_estado)
        _ESTADO_TENDENCIA[symbol_key] = nuevo_estado
    estrategias = obtener_estrategias_por_tendencia(tendencia)
    estrategias_activas = (
        {nombre: (True) for nombre in estrategias}
        if isinstance(estrategias, list)
        else estrategias if isinstance(estrategias, dict) else {}
    )
    if emitir_log:
        log.info(
            {
                "evento": "deteccion_tendencia",
                "symbol": symbol,
                "tendencia": tendencia,
                "delta_ema": round(delta, 6),
                "slope_local": round(slope, 6),
                "rsi": round(rsi, 2) if rsi is not None else None,
                "adx": round(adx, 2) if adx is not None else None,
            }
        )
    return tendencia, estrategias_activas


def obtener_tendencia(symbol: str, df: pd.DataFrame) -> str:
    """Devuelve la tendencia si hay suficientes datos, sino pospone."""

    if df is None or df.empty or "close" not in df.columns:
        return "lateral"
    update_progress(symbol, len(df))
    if len(df) < MIN_BARS:
        enqueue_fetch(symbol)
        if not was_warned(symbol):
            log.info(
                f"⏳ Warmup {symbol}: {len(df)}/{MIN_BARS} velas; posponiendo evaluación"
            )
            mark_warned(symbol)
        if len(df) < 30:
            return "lateral"

    tendencia, _ = detectar_tendencia(symbol, df)
    return tendencia


def obtener_parametros_persistencia(
    tendencia: str, volatilidad: float
) -> tuple[float, int]:
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
        symbol_dbg = None
        try:
            ventana = df.iloc[i - 30 : i]
            if ventana.empty or len(ventana) < 10:
                continue
            symbol_dbg = (
                df.iloc[i].get("symbol") if "symbol" in df.columns else "<desconocido>"
            )
            symbol = symbol_dbg
            tendencia, _ = detectar_tendencia(symbol, ventana, registrar=False)
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
            log.warning(
                f"⚠️ Fallo al evaluar repetición de señales en {symbol_dbg}: {e}"
            )
            continue
        finally:
            await asyncio.sleep(0)
    return contador
