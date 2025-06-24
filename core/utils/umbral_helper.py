from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import linregress
from ta.momentum import RSIIndicator

from core.contexto_externo import obtener_puntaje_contexto

UMBRAL_POR_DEFECTO = 10
MIN_LONGITUD_DATA = 30
PESO_VOLATILIDAD = 0.4
PESO_RANGO = 0.4
PESO_VOLUMEN = 0.2


def _limites_adaptativos(contexto_score: float) -> tuple[float, float]:
    base_max = 10.0
    base_min = 1.0
    umbral_max = max(5.0, min(30.0, base_max + contexto_score))
    umbral_min = max(0.5, base_min + contexto_score * 0.5)
    if umbral_max < umbral_min:
        umbral_max = umbral_min + 1.0
    return umbral_max, umbral_min


def _calcular_metricas(df: pd.DataFrame) -> dict:
    ventana_close = df["close"].tail(10)
    ventana_high = df["high"].tail(10)
    ventana_low = df["low"].tail(10)
    ventana_vol = df["volume"].tail(30)

    media_close = np.mean(ventana_close)
    if media_close == 0 or np.isnan(media_close):
        volatilidad = 0.0
        rango_medio = 0.0
    else:
        volatilidad = np.std(ventana_close) / media_close
        rango_medio = np.mean(ventana_high - ventana_low) / media_close

    volumen_promedio = ventana_vol.mean()
    volumen_max = ventana_vol.max()
    volumen_relativo = 0.5 if volumen_max == 0 or np.isnan(volumen_max) else volumen_promedio / volumen_max

    momentum_std = df["close"].pct_change().tail(5).std()

    try:
        slope = linregress(range(len(ventana_close)), ventana_close).slope
    except ValueError:
        slope = 0.0

    try:
        rsi = RSIIndicator(close=df["close"], window=14).rsi().iloc[-1]
    except Exception:
        rsi = 50.0

    return {
        "volatilidad": float(volatilidad),
        "rango_medio": float(rango_medio),
        "volumen_relativo": float(volumen_relativo),
        "momentum_std": float(momentum_std),
        "slope": float(slope),
        "rsi": float(rsi),
    }


def calcular_umbral_avanzado(
    symbol: str,
    df: pd.DataFrame,
    estrategias_activadas: dict | None,
    pesos_symbol: dict | None,
    persistencia: float = 0.0,
    config: dict | None = None,
    *,
    pesos_componentes: dict | None = None,
) -> float:
    if df is None or len(df) < MIN_LONGITUD_DATA or not estrategias_activadas:
        return UMBRAL_POR_DEFECTO
    if not {"close", "high", "low", "volume"}.issubset(df.columns):
        return UMBRAL_POR_DEFECTO

    pesos_componentes = pesos_componentes or {
        "volatilidad": PESO_VOLATILIDAD,
        "rango_medio": PESO_RANGO,
        "volumen_relativo": PESO_VOLUMEN,
        "momentum_std": 0.2,
    }

    m = _calcular_metricas(df)

    if config:
        ajuste_volatilidad = config.get("ajuste_volatilidad", 1.0)
        factor_umbral = config.get("factor_umbral", 1.0)
        ajuste_riesgo = config.get("riesgo_maximo_diario", 1.0)
    else:
        ajuste_volatilidad = 1.0
        factor_umbral = 1.0
        ajuste_riesgo = 1.0

    contexto_score = (
        (
            m["volatilidad"] * pesos_componentes["volatilidad"]
            + m["rango_medio"] * pesos_componentes["rango_medio"]
            + m["volumen_relativo"] * pesos_componentes["volumen_relativo"]
            + m["momentum_std"] * pesos_componentes.get("momentum_std", 0.2)
        )
        * 10
    ) * ajuste_volatilidad

    if 40 < m["rsi"] < 60:
        penalizacion = 1 - (1 - abs(m["rsi"] - 50) / 10) * 0.25
        contexto_score *= penalizacion

    contexto_extra = obtener_puntaje_contexto(symbol)
    try:
        contexto_score += float(contexto_extra)
    except (TypeError, ValueError):
        pass

    pesos_validos = [
        pesos_symbol.get(k, 0)
        for k in estrategias_activadas
        if pesos_symbol and pesos_symbol.get(k, 0) > 0
    ]
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
    if m["slope"] < 0:
        ajuste_riesgo += 0.2

    dinamica_persistencia = 0.05 + min(abs(m["slope"]) * 0.1, 0.15) + min(m["momentum_std"] * 2, 0.1)
    factor_persistencia = 1 - min(persistencia * dinamica_persistencia, 0.3)

    max_dinamico, min_dinamico = _limites_adaptativos(contexto_score)
    umbral_base = min(potencia_tecnica * ajuste_riesgo, max_dinamico)
    umbral = max(
        min(umbral_base * factor_umbral * factor_persistencia, max_dinamico),
        min_dinamico,
    )
    return float(umbral)