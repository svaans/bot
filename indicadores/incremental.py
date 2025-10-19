from __future__ import annotations

from collections import deque
from typing import Any

import pandas as pd

from indicadores.atr import calcular_atr
from indicadores.helpers import filtrar_cerradas, serie_cierres, set_cached_value


def _ensure_state_cache(estado: Any) -> dict[str, Any]:
    """Garantiza un contenedor ``dict`` para caches incrementales."""

    if isinstance(estado, dict):
        cache = estado.get("indicadores_cache")
        if not isinstance(cache, dict):
            cache = {}
            estado["indicadores_cache"] = cache
        return cache
    
    cache = getattr(estado, "indicators_state", None)
    if isinstance(cache, dict):
        try:
            setattr(estado, "indicadores_cache", cache)
        except Exception:
            pass
        return cache

    cache = getattr(estado, "indicadores_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        try:
            setattr(estado, "indicadores_cache", cache)
        except Exception:
            pass
    return cache


def _resolve_df(estado: Any, df: pd.DataFrame | None) -> pd.DataFrame | None:
    if isinstance(df, pd.DataFrame):
        return df
    if isinstance(estado, dict):
        candidato = estado.get("df")
        if isinstance(candidato, pd.DataFrame):
            return candidato
        candidato = estado.get("last_df")
        if isinstance(candidato, pd.DataFrame):
            return candidato
    candidato = getattr(estado, "df", None)
    if isinstance(candidato, pd.DataFrame):
        return candidato
    candidato = getattr(estado, "last_df", None)
    if isinstance(candidato, pd.DataFrame):
        return candidato
    return None


def actualizar_rsi_incremental(
    estado: Any,
    df: pd.DataFrame | None = None,
    periodo: int = 14,
) -> float | None:
    """Actualiza el RSI de forma incremental y devuelve el valor calculado."""

    df_resuelto = _resolve_df(estado, df)
    if df_resuelto is None:
        return None

    serie = serie_cierres(df_resuelto)
    if serie is None or len(serie) < periodo + 1:
        return None

    serie = serie.astype(float)
    cache_global = _ensure_state_cache(estado)
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
        epsilon = 1e-10
        denom = float(avg_loss) + epsilon
        rs = float(avg_gain) / denom
        rsi = 100 - 100 / (1 + rs)
    else:
        prev_close = float(datos_rsi["prev_close"])
        delta = ultimo_cierre - prev_close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = (datos_rsi["avg_gain"] * (periodo - 1) + gain) / periodo
        avg_loss = (datos_rsi["avg_loss"] * (periodo - 1) + loss) / periodo
        epsilon = 1e-10
        denom = float(avg_loss) + epsilon
        rs = float(avg_gain) / denom
        rsi = 100 - 100 / (1 + rs)

    rsi = max(0.0, min(100.0, float(rsi)))
    
    cache_global["rsi"] = {
        "periodo": periodo,
        "avg_gain": float(avg_gain),
        "avg_loss": float(avg_loss),
        "prev_close": ultimo_cierre,
        "valor": float(rsi),
    }

    set_cached_value(df_resuelto, ("rsi", periodo, False), float(rsi))
    return float(rsi)


def actualizar_momentum_incremental(
    estado: Any,
    df: pd.DataFrame | None = None,
    periodo: int = 10,
) -> float:
    """Actualiza el *momentum* de forma incremental y devuelve el valor."""

    df_resuelto = _resolve_df(estado, df)
    if df_resuelto is None:
        return 0.0

    serie = serie_cierres(df_resuelto)
    if serie is None or len(serie) < periodo + 1:
        return 0.0

    serie = serie.astype(float)
    cache_global = _ensure_state_cache(estado)
    datos = cache_global.get("momentum")
    ultimo_cierre = float(serie.iloc[-1])

    if (
        not datos
        or datos.get("periodo") != periodo
        or len(serie) <= periodo
    ):
        cierres = deque(
            serie.tail(periodo + 1).tolist(),
            maxlen=periodo + 1,
        )
        if len(cierres) < periodo + 1:
            return 0.0
    else:
        cierres = datos["cierres"]
        cierres.append(ultimo_cierre)
        if len(cierres) < periodo + 1:
            return 0.0

    referencia = cierres[0]
    if not referencia:
        momentum = 0.0
    else:
        momentum = (ultimo_cierre / referencia) - 1
    momentum = max(-1.0, min(1.0, float(momentum)))

    cache_global["momentum"] = {
        "periodo": periodo,
        "cierres": cierres,
        "valor": float(momentum),
    }
    set_cached_value(df_resuelto, ("momentum", periodo), float(momentum))
    return float(momentum)


def actualizar_atr_incremental(
    estado: Any,
    df: pd.DataFrame | None = None,
    periodo: int = 14,
) -> float | None:
    """Actualiza el ATR de forma incremental y devuelve el valor."""

    df_resuelto = _resolve_df(estado, df)
    columnas = {"high", "low", "close"}
    if (
        df_resuelto is None
        or df_resuelto.empty
        or not columnas.issubset(df_resuelto.columns)
    ):
        return None

    df_filtrado = filtrar_cerradas(df_resuelto)
    if len(df_filtrado) < periodo + 1:
        return None

    cache_global = _ensure_state_cache(estado)
    datos = cache_global.get("atr")
    h = float(df_filtrado["high"].iloc[-1])
    l = float(df_filtrado["low"].iloc[-1])
    c = float(df_filtrado["close"].iloc[-1])

    if (
        not datos
        or datos.get("periodo") != periodo
        or len(df_filtrado) <= periodo
    ):
        atr_val = calcular_atr(df_filtrado, periodo)
        if atr_val is None:
            return None
        cache_global["atr"] = {
            "periodo": periodo,
            "prev_close": c,
            "valor": float(atr_val),
        }
        set_cached_value(df_resuelto, ("atr", periodo), float(atr_val))
        return float(atr_val)

    prev_close = float(datos["prev_close"])
    tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
    atr = (datos["valor"] * (periodo - 1) + tr) / periodo
    cache_global["atr"] = {
        "periodo": periodo,
        "prev_close": c,
        "valor": float(atr),
    }

    set_cached_value(df_resuelto, ("atr", periodo), float(atr))
    return float(atr)
