"""Utilidades compartidas para ajustar ratios de SL/TP, riesgo y modo agresivo."""

from __future__ import annotations

import os
import re
from typing import Any

from core.utils.utils import configurar_logger

# Umbrales y riesgos base unificados
MODO_AGRESIVO_VOL_THRESHOLD = 0.02
MODO_AGRESIVO_SLOPE_THRESHOLD = 0.002
RIESGO_POR_TRADE_BASE = 0.02  # 2% por operación
RIESGO_MAXIMO_DIARIO_BASE = 0.06  # 6% riesgo global diario

log = configurar_logger("ajustador_riesgo")


_ENV_VOL_THRESHOLD = "MODO_AGRESIVO_VOL_THRESHOLD"
_ENV_SLOPE_THRESHOLD = "MODO_AGRESIVO_SLOPE_THRESHOLD"


def _coerce_float(value: Any, default: float) -> float:
    """Convierte ``value`` a ``float`` manejando entradas no válidas."""

    if value is None:
        return default
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):  # pragma: no cover - defensivo
            return default
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return default
        try:
            return float(cleaned)
        except ValueError:
            log.warning("Valor de umbral no numérico: %s", value)
            return default
    return default


def _normalizar_simbolo(symbol: str) -> str:
    """Normaliza el símbolo para usarlo en variables de entorno."""

    return re.sub(r"[^A-Z0-9]", "", symbol.upper())


def _resolver_umbral_env(nombre: str, symbol: str | None, default: float) -> float:
    """Obtiene un umbral desde variables de entorno con fallback."""

    if symbol:
        clave_simbolo = f"{nombre}_{_normalizar_simbolo(symbol)}"
        if clave_simbolo in os.environ:
            return _coerce_float(os.environ.get(clave_simbolo), default)
    if nombre in os.environ:
        return _coerce_float(os.environ.get(nombre), default)
    return default


def es_modo_agresivo(
    volatilidad: float,
    slope_pct: float,
    *,
    symbol: str | None = None,
    vol_threshold: Any | None = None,
    slope_threshold: Any | None = None,
) -> bool:
    """Determina si el bot debe operar en modo agresivo.

    Los umbrales se resuelven con la siguiente prioridad (mayor a menor):

    1. Valores proporcionados directamente (por símbolo) via parámetros.
    2. Variables de entorno específicas por símbolo.
    3. Variables de entorno globales.
    4. Constantes predeterminadas del módulo.
    """

    vol_por_defecto = _resolver_umbral_env(
        _ENV_VOL_THRESHOLD, symbol, MODO_AGRESIVO_VOL_THRESHOLD
    )
    slope_por_defecto = _resolver_umbral_env(
        _ENV_SLOPE_THRESHOLD, symbol, MODO_AGRESIVO_SLOPE_THRESHOLD
    )
    vol_thr = _coerce_float(vol_threshold, vol_por_defecto)
    slope_thr = _coerce_float(slope_threshold, slope_por_defecto)
    return volatilidad > vol_thr or abs(slope_pct) > slope_thr


def ajustar_sl_tp_riesgo(
    volatilidad: float,
    slope_pct: float,
    base_riesgo: float = RIESGO_MAXIMO_DIARIO_BASE,
    sl_ratio: float = 1.5,
    tp_ratio: float = 3.0,
    *,
    regime: str | None = None,
) -> tuple[float, float, float]:
    """Calcula ratios de SL/TP y riesgo adaptativos.

    Parameters
    ----------
    volatilidad:
        Medida de volatilidad relativa (ej. desviación estándar o ATR normalizado).
    slope_pct:
        Pendiente porcentual que refleja la direccionalidad reciente del precio.
    base_riesgo:
        Riesgo máximo diario base que se ajustará según contexto.
    sl_ratio:
        Ratio base para el ``stop loss`` expresado en múltiplos de ATR u otra métrica.
    tp_ratio:
        Ratio base para el ``take profit``.
    regime:
        Clasificación cualitativa del mercado (``alta_volatilidad``, ``tendencial`` o
        ``lateral``). Si no se proporciona se mantiene el comportamiento previo.
    """

    riesgo_inicial = base_riesgo
    # Ajuste por volatilidad
    if volatilidad > 0.02:
        sl_ratio *= 1.2
        base_riesgo *= 0.8
    elif volatilidad < 0.01 and abs(slope_pct) > 0.001:
        base_riesgo *= 1.2

    # Ajuste por slope
    if slope_pct > 0.001:
        tp_ratio *= 1.1
    elif slope_pct < -0.001:
        tp_ratio *= 0.9
        sl_ratio *= 1.1

    regime_key = (regime or "").strip().lower()
    regime_aplicado = None
    if regime_key:
        if regime_key == "alta_volatilidad":
            sl_ratio *= 1.1
            tp_ratio *= 0.95
            base_riesgo *= 0.7
            regime_aplicado = regime_key
        elif regime_key == "tendencial":
            sl_ratio *= 0.95
            tp_ratio *= 1.15
            base_riesgo *= 1.05
            regime_aplicado = regime_key
        elif regime_key == "lateral":
            tp_ratio *= 0.9
            base_riesgo *= 0.9
            regime_aplicado = regime_key
        else:  # pragma: no cover - rutas defensivas
            log.debug("Regimen de mercado desconocido: %s", regime)

    sl_ratio = min(max(sl_ratio, 0.5), 5.0)
    tp_ratio = max(tp_ratio, sl_ratio * 1.2)
    tp_ratio = min(max(tp_ratio, 1.0), 8.0)
    riesgo_maximo_diario = round(base_riesgo, 4)
    if regime_aplicado:
        log.info(
            "Regimen '%s' aplicado | SL=%.2f | TP=%.2f | Riesgo=%.4f",
            regime_aplicado,
            round(sl_ratio, 2),
            round(tp_ratio, 2),
            riesgo_maximo_diario,
        )
    if riesgo_maximo_diario != round(riesgo_inicial, 4):
        log.info(
            "Riesgo diario ajustado de %.4f a %.4f",
            round(riesgo_inicial, 4),
            riesgo_maximo_diario,
        )
    return round(sl_ratio, 2), round(tp_ratio, 2), riesgo_maximo_diario