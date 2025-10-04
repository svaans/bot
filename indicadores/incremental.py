from __future__ import annotations

from collections import deque

from math import isclose
import pandas as pd

from indicators.atr import calcular_atr

def actualizar_rsi_incremental(estado, periodo: int = 14) -> float | None:
    """Actualiza el RSI de forma incremental y devuelve el valor calculado.

    El resultado se almacena en ``estado.indicadores_cache`` y también en
    ``estado.df.attrs['_indicators_cache']`` para que las funciones helper
    puedan reutilizarlo sin recomputar todo el historial.  
    """
    df = getattr(estado, "df", None)
    if df is None or df.empty or "close" not in df.columns:
        return None

    cache_global = getattr(estado, "indicadores_cache", None)
    if not isinstance(cache_global, dict):
        cache_global = {}
        setattr(estado, "indicadores_cache", cache_global)
    datos_rsi = cache_global.get("rsi")
    ultimo_cierre = float(df["close"].iloc[-1])

    if not datos_rsi or datos_rsi.get("periodo") != periodo or len(df) <= periodo:
        # Inicialización: calcular RSI completo y promedios
        delta = df["close"].diff()
        ganancia = delta.clip(lower=0)
        perdida = -delta.clip(upper=0)
        avg_gain = (
            ganancia.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo)
            .mean()
            .iloc[-1]
        )
        avg_loss = (
            perdida.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo)
            .mean()
            .iloc[-1]
        )
        if isclose(avg_loss, 0.0, rel_tol=1e-12, abs_tol=1e-12):
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
    else:
        prev_close = datos_rsi["prev_close"]
        delta = ultimo_cierre - prev_close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = (datos_rsi["avg_gain"] * (periodo - 1) + gain) / periodo
        avg_loss = (datos_rsi["avg_loss"] * (periodo - 1) + loss) / periodo
        if isclose(avg_loss, 0.0, rel_tol=1e-12, abs_tol=1e-12):
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
    cache_global["rsi"] = {
        "periodo": periodo,
        "avg_gain": float(avg_gain),
        "avg_loss": float(avg_loss),
        "prev_close": ultimo_cierre,
        "valor": float(rsi),
    }

    df.attrs.setdefault("_indicators_cache", {})[("rsi", periodo, False)] = float(rsi)
    return float(rsi)


def actualizar_momentum_incremental(estado, periodo: int = 10) -> float | None:
    """Actualiza el *momentum* de forma incremental y devuelve el valor."""

    df = getattr(estado, "df", None)
    if df is None or df.empty or "close" not in df.columns:
        return None

    cache_global = getattr(estado, "indicadores_cache", None)
    if not isinstance(cache_global, dict):
        cache_global = {}
        setattr(estado, "indicadores_cache", cache_global)
    datos = cache_global.get("momentum")
    ultimo_cierre = float(df["close"].iloc[-1])

    if not datos or datos.get("periodo") != periodo or len(df) <= periodo:
        cierres = deque(df["close"].tail(periodo + 1).tolist(), maxlen=periodo + 1)
        if len(cierres) < periodo + 1:
            return None
        momentum = (ultimo_cierre / cierres[0]) - 1
    else:
        cierres = datos["cierres"]
        cierres.append(ultimo_cierre)
        if len(cierres) < periodo + 1:
            return None
        momentum = (ultimo_cierre / cierres[0]) - 1

    cache_global["momentum"] = {
        "periodo": periodo,
        "cierres": cierres,
        "valor": float(momentum),
    }
    df.attrs.setdefault("_indicators_cache", {})[("momentum", periodo)] = float(momentum)
    return float(momentum)


def actualizar_atr_incremental(estado, periodo: int = 14) -> float | None:
    """Actualiza el ATR de forma incremental y devuelve el valor."""

    df = getattr(estado, "df", None)
    columnas = {"high", "low", "close"}
    if df is None or df.empty or not columnas.issubset(df.columns):
        return None

    cache_global = getattr(estado, "indicadores_cache", None)
    if not isinstance(cache_global, dict):
        cache_global = {}
        setattr(estado, "indicadores_cache", cache_global)
    datos = cache_global.get("atr")
    h = float(df["high"].iloc[-1])
    l = float(df["low"].iloc[-1])
    c = float(df["close"].iloc[-1])

    if not datos or datos.get("periodo") != periodo or len(df) <= periodo:
        atr_val = calcular_atr(df, periodo)
        if atr_val is None:
            return None
        cache_global["atr"] = {
            "periodo": periodo,
            "prev_close": c,
            "valor": float(atr_val),
        }
        df.attrs.setdefault("_indicators_cache", {})[("atr", periodo)] = float(atr_val)
        return float(atr_val)

    prev_close = datos["prev_close"]
    tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
    atr = (datos["valor"] * (periodo - 1) + tr) / periodo
    cache_global["atr"] = {
        "periodo": periodo,
        "prev_close": c,
        "valor": float(atr),
    }

    df.attrs.setdefault("_indicators_cache", {})[("atr", periodo)] = float(atr)
    return float(atr)