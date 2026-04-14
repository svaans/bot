"""Clasificación ligera de régimen (volatilidad) para ajustar umbrales de entrada.

Fase 5 del plan: condicionar exigencia de scores sin cambiar la lógica de señales.
Se desactiva por defecto (``regimen_entrada_enabled=False``).
"""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


def atr_ratio_actual(df: pd.DataFrame, period: int = 14) -> float:
    """ATR simple / precio de cierre (adimensional, ~ volatilidad relativa)."""

    if df is None or len(df) < period + 1:
        return 0.0
    need = {"high", "low", "close"}
    if not need.issubset(df.columns):
        return 0.0
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(period, min_periods=period).mean().iloc[-1]
    c = float(close.iloc[-1])
    if c <= 0 or pd.isna(atr):
        return 0.0
    return float(atr) / c


def etiqueta_volatilidad(
    df: pd.DataFrame,
    *,
    umbral_alto: float = 0.025,
    umbral_bajo: float = 0.008,
    periodo_atr: int = 14,
) -> str:
    """``alta`` | ``media`` | ``baja`` según ATR/cierre."""

    r = atr_ratio_actual(df, period=periodo_atr)
    if r >= umbral_alto:
        return "alta"
    if r <= umbral_bajo:
        return "baja"
    return "media"


def aplicar_multiplicadores_regimen(
    umbral: float,
    umbral_score: float,
    vol: str,
    config: Mapping[str, Any],
) -> tuple[float, float]:
    """Escala umbrales según etiqueta de volatilidad y multiplicadores en ``config``."""

    if not bool(config.get("regimen_entrada_enabled", False)):
        return umbral, umbral_score
    mu_alta = float(config.get("regimen_mult_umbral_alta", 1.0) or 1.0)
    mu_media = float(config.get("regimen_mult_umbral_media", 1.0) or 1.0)
    mu_baja = float(config.get("regimen_mult_umbral_baja", 1.0) or 1.0)
    ms_alta = float(config.get("regimen_mult_umbral_score_alta", 1.0) or 1.0)
    ms_media = float(config.get("regimen_mult_umbral_score_media", 1.0) or 1.0)
    ms_baja = float(config.get("regimen_mult_umbral_score_baja", 1.0) or 1.0)
    map_u = {"alta": mu_alta, "media": mu_media, "baja": mu_baja}
    map_s = {"alta": ms_alta, "media": ms_media, "baja": ms_baja}
    u = float(umbral) * float(map_u.get(vol, 1.0))
    us = float(umbral_score) * float(map_s.get(vol, 1.0))
    return u, us
