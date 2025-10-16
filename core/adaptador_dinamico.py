"""Herramientas para ajustar parámetros técnicos de forma dinámica.

Provee utilidades para adaptar configuraciones base en función de la
volatilidad reciente y para calcular niveles de ``take profit`` y ``stop
loss`` según el régimen de mercado.
"""
from __future__ import annotations
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List
import numpy as np
import pandas as pd

from data_feed.candle_builder import backfill
from indicadores.retornos_volatilidad import (
    retornos_log,
    retornos_simples,
    verificar_consistencia,
    volatilidad_welford,
)
from core.ajustador_riesgo import (
    RIESGO_MAXIMO_DIARIO_BASE,
    ajustar_sl_tp_riesgo,
    es_modo_agresivo,
)
from core.market_regime import detectar_regimen
from core.utils.market_utils import calcular_slope_pct
from core.utils.utils import configurar_logger
log = configurar_logger('adaptador_dinamico')
RUTA_CONFIGS_OPTIMAS = 'config/configuraciones_optimas.json'
if os.path.exists(RUTA_CONFIGS_OPTIMAS):
    with open(RUTA_CONFIGS_OPTIMAS, 'r') as f:
        CONFIGS_OPTIMAS = json.load(f)
else:
    log.warning(
        '❌ Archivo de configuración no encontrado. Se usará configuración por defecto.'
    )
    CONFIGS_OPTIMAS: dict = {}

@dataclass(frozen=True)
class MinDistanceConstraints:
    """Parámetros efectivos de distancia mínima para SL/TP."""

    min_distance_pct: float
    min_distance_ticks: int
    tick_size: float
    min_distance_abs: float


@dataclass(frozen=True)
class TpSlResult:
    """Resultado estructurado del cálculo de SL/TP adaptativos."""

    sl: float
    tp: float
    atr: float
    multiplicador_sl: float
    multiplicador_tp: float
    min_constraints: MinDistanceConstraints
    sl_offset: float
    tp_offset: float
    sl_clamped: bool
    tp_clamped: bool

    def __iter__(self):  # pragma: no cover - compatibilidad con tuple unpacking
        yield self.sl
        yield self.tp


def _extraer_tick_size(config: Dict[str, Any]) -> float:
    """Obtiene ``tick_size`` desde la configuración si está disponible."""

    filtros = config.get('filters') if isinstance(config.get('filters'), dict) else {}
    filtros_mercado = (
        config.get('market_filters') if isinstance(config.get('market_filters'), dict) else {}
    )
    candidatos = [
        config.get('tick_size'),
        config.get('tickSize'),
        filtros.get('tick_size'),
        filtros.get('tickSize'),
        filtros_mercado.get('tick_size'),
        filtros_mercado.get('tickSize'),
    ]
    for candidato in candidatos:
        if candidato in (None, ''):
            continue
        try:
            tick = float(candidato)
            if tick > 0:
                return tick
        except (TypeError, ValueError):  # pragma: no cover - defensivo
            continue
    return 0.0


def _resolver_min_distancia(
    config: Dict[str, Any],
    precio_actual: float,
) -> MinDistanceConstraints:
    """Calcula las restricciones efectivas de distancia mínima."""

    min_pct_cfg = config.get('distancia_minima_pct', config.get('min_distancia_pct'))
    if min_pct_cfg is None:
        min_pct_cfg = os.getenv('MIN_DISTANCIA_SL_TP_PCT', '0.0005')
    try:
        min_pct_cfg = float(min_pct_cfg)
    except (TypeError, ValueError):
        min_pct_cfg = 0.0005
    min_pct_cfg = max(min_pct_cfg, 1e-6)

    min_ticks_cfg = config.get('min_distancia_ticks', os.getenv('MIN_DISTANCIA_TICKS'))
    try:
        min_ticks = max(1, int(min_ticks_cfg))
    except (TypeError, ValueError):
        min_ticks = 2

    tick_size = _extraer_tick_size(config)

    min_pct_ticks = (tick_size * min_ticks) / precio_actual if precio_actual > 0 else 0.0
    min_pct = max(min_pct_cfg, min_pct_ticks)
    min_distance_pct = min_pct if precio_actual > 0 else min_pct_cfg
    if precio_actual > 0:
        min_distance_abs_pct = precio_actual * min_distance_pct
    else:
        min_distance_abs_pct = min_distance_pct
    min_distance_abs = max(min_distance_abs_pct, tick_size * min_ticks)

    return MinDistanceConstraints(
        min_distance_pct=min_distance_pct,
        min_distance_ticks=min_ticks,
        tick_size=tick_size,
        min_distance_abs=min_distance_abs,
    )


def _adaptar_configuracion_base(symbol: str, df: pd.DataFrame, base_config: dict
    ) ->dict:
    """Ajusta ``base_config`` dinámicamente en función del mercado."""
    if df is None or len(df) < 11 or 'close' not in df.columns:
        log.warning(
            f'[{symbol}] ❌ Datos insuficientes para adaptar configuración.')
        return base_config
    config = base_config.copy()
    precios = df['close'].tail(11)
    simples = retornos_simples(precios)
    logs = retornos_log(precios)
    if verificar_consistencia(simples, logs):
        volatilidad = volatilidad_welford(simples.tail(10))
    else:
        volatilidad = 0
    cierre_reciente = df['close'].tail(10)
    slope_pct = calcular_slope_pct(cierre_reciente)
    precio_ref = cierre_reciente.iloc[-1] if len(cierre_reciente) else 0.0
    slope = slope_pct * precio_ref
    config['ajuste_volatilidad'] = round(min(5.0, 1 + volatilidad * 10), 2)
    factor_dinamico = base_config.get('factor_umbral', 1.0) * (1 + volatilidad)
    config['factor_umbral'] = round(min(3.0, max(0.3, factor_dinamico)), 2)
    sl_base = base_config.get('sl_ratio', 1.5)
    tp_base = base_config.get('tp_ratio', 3.0)
    riesgo_base = base_config.get('riesgo_maximo_diario', RIESGO_MAXIMO_DIARIO_BASE)
    sl_ratio, tp_ratio, riesgo_diario = ajustar_sl_tp_riesgo(
        volatilidad, slope_pct, riesgo_base, sl_base, tp_base
    )
    if round(riesgo_diario, 4) != round(riesgo_base, 4):
        log.info(
            f"[{symbol}] Riesgo diario ajustado a {riesgo_diario:.4f}"
        )
    config['sl_ratio'] = sl_ratio
    config['tp_ratio'] = tp_ratio
    base_peso = base_config.get('peso_minimo_total', 0.5)
    config['peso_minimo_total'] = round(min(5.0, base_peso * (1 +
        volatilidad * 1.5)), 2)
    base_div = base_config.get('diversidad_minima', 2)
    diversidad = base_div
    if slope > 0.002:
        diversidad = max(1, base_div - 1)
    elif slope < -0.002:
        diversidad = max(base_div, 2)
    cooldown = min(24, max(0, int(volatilidad * 100)))
    modo_agresivo = es_modo_agresivo(
        volatilidad,
        slope_pct,
        symbol=symbol,
        vol_threshold=base_config.get('modo_agresivo_vol_threshold'),
        slope_threshold=base_config.get('modo_agresivo_slope_threshold'),
    )
    if modo_agresivo:
        log.info(
            f"[{symbol}] Modo agresivo activado (Vol={volatilidad:.4f}, Slope%={slope_pct:.4f})"
        )
        diversidad = max(1, base_div - 1)
    config['diversidad_minima'] = diversidad
    config['cooldown_tras_perdida'] = cooldown
    config['modo_agresivo'] = modo_agresivo
    config['ponderar_por_diversidad'] = config['diversidad_minima'] <= 2
    base_mult = base_config.get('multiplicador_estrategias_recurrentes', 1.5)
    config['multiplicador_estrategias_recurrentes'] = round(min(3.0,
        base_mult * (1 + volatilidad)), 2)
    config['riesgo_maximo_diario'] = riesgo_diario
    log.info(
        f"[{symbol}] Config adaptada | Volatilidad={volatilidad:.4f} | Slope={slope:.4f} | Factor Umbral={config['factor_umbral']} | SL={config['sl_ratio']} | TP={config['tp_ratio']} | PesoMin={config['peso_minimo_total']} | Diversidad={config['diversidad_minima']} | Cooldown={config['cooldown_tras_perdida']} | Riesgo Diario={config['riesgo_maximo_diario']} | Aggresivo={config['modo_agresivo']}"
        )
    return config


def calcular_tp_sl_adaptativos(
    symbol: str,
    df: pd.DataFrame,
    config: dict | None = None,
    capital_actual: float | None = None,
    precio_actual: float | None = None,
) -> TpSlResult:
    if config is None:
        config = {}
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df debe ser un DataFrame de pandas')
    if precio_actual is None:
        precio_actual = float(df['close'].iloc[-1])
    min_constraints = _resolver_min_distancia(config, precio_actual)
    columnas_requeridas = {'high', 'low', 'close'}
    if not columnas_requeridas.issubset(df.columns):
        log.warning(
            f'[{symbol}] ❌ Columnas insuficientes para TP/SL. Usando margen fijo.'
        )
        margen_base = precio_actual * 0.01
        offset = max(margen_base, min_constraints.min_distance_abs)
        sl = round(max(0.0, precio_actual - offset), 6)
        tp = round(precio_actual + offset, 6)
        clamped = offset > margen_base
        return TpSlResult(
            sl=sl,
            tp=tp,
            atr=float('nan'),
            multiplicador_sl=float('nan'),
            multiplicador_tp=float('nan'),
            min_constraints=min_constraints,
            sl_offset=offset,
            tp_offset=offset,
            sl_clamped=clamped,
            tp_clamped=clamped,
        )

    trabajo = df.copy()
    trabajo = trabajo.ffill().bfill()
    trabajo['hl'] = trabajo['high'] - trabajo['low']
    trabajo['hc'] = abs(trabajo['high'] - trabajo['close'].shift(1))
    trabajo['lc'] = abs(trabajo['low'] - trabajo['close'].shift(1))
    trabajo['tr'] = trabajo[['hl', 'hc', 'lc']].max(axis=1)

    regimen = detectar_regimen(trabajo)
    ventana_atr = 7 if regimen == 'lateral' else 14
    atr = trabajo['tr'].rolling(window=ventana_atr).mean().iloc[-1]
    if pd.isna(atr) or atr <= 0:
        atr = precio_actual * 0.01
    multiplicador_sl = config.get('sl_ratio', 1.5)
    multiplicador_tp = config.get('tp_ratio', 2.5)
    if regimen == 'lateral':
        multiplicador_sl *= 0.8
        multiplicador_tp *= 0.8
    else:
        multiplicador_sl *= 1.2
        multiplicador_tp *= 1.2
    if config.get('modo_capital_bajo') and capital_actual is not None and capital_actual < 500:
        factor = 1 + (1 - capital_actual / 500) * 0.2
        multiplicador_tp *= factor
        multiplicador_sl *= max(0.5, 1 - (1 - capital_actual / 500) * 0.1)
    sl_offset_base = atr * multiplicador_sl
    tp_offset_base = atr * multiplicador_tp
    sl_clamped = sl_offset_base < min_constraints.min_distance_abs
    tp_clamped = tp_offset_base < min_constraints.min_distance_abs
    sl_offset = max(sl_offset_base, min_constraints.min_distance_abs)
    tp_offset = max(tp_offset_base, min_constraints.min_distance_abs)

    sl = round(max(0.0, precio_actual - sl_offset), 6)
    tp = round(precio_actual + tp_offset, 6)
    log.debug(
        f'[{symbol}] TP/SL adaptativos | Regimen: {regimen} | Precio: {precio_actual:.2f} | ATR: {atr:.5f} | '
        f'SL: {sl:.2f} | TP: {tp:.2f} | Ratios: SL x{multiplicador_sl}, TP x{multiplicador_tp} | '
        f'Capital: {capital_actual} | MinDistAbs: {min_constraints.min_distance_abs:.6f}'
    )

    return TpSlResult(
        sl=sl,
        tp=tp,
        atr=float(atr),
        multiplicador_sl=float(multiplicador_sl),
        multiplicador_tp=float(multiplicador_tp),
        min_constraints=min_constraints,
        sl_offset=float(sl_offset),
        tp_offset=float(tp_offset),
        sl_clamped=bool(sl_clamped),
        tp_clamped=bool(tp_clamped),
    )


async def backfill_ventana(
    symbol: str,
    *,
    intervalo: str,
    cliente: Any | None,
    start_ts: int,
    candles: int,
    current_ts: int | None = None,
    max_candles: int = 1000,
    fetcher: Any | None = None,
) -> List[Dict[str, float]]:
    """Obtiene velas intermedias tras una reconexión limitada a ``max_candles``.

    Parameters
    ----------
    symbol:
        Símbolo a rellenar.
    intervalo:
        Timeframe textual (``1m``, ``5m``...).
    cliente:
        Cliente REST de Binance reutilizado por el ``DataFeed``.
    start_ts:
        Timestamp (ms) esperado para la primera vela faltante.
    candles:
        Cantidad estimada de velas a recuperar.
    current_ts:
        Timestamp de la vela recibida que disparó el backfill. Se excluye de los
        resultados para evitar duplicados.
    max_candles:
        Límite superior de velas a solicitar en esta operación.
    fetcher:
        Callable opcional compatible con :func:`data_feed.candle_builder.backfill`.
    """
    if candles <= 0 or max_candles <= 0:
        return []

    limit = min(int(candles), int(max_candles))
    if limit <= 0:
        return []

    try:
        fetched = await backfill(
            symbol,
            limit,
            intervalo=intervalo,
            cliente=cliente,
            fetcher=fetcher,
        )
    except Exception:
        log.exception(
            "backfill_ventana.error",
            extra={"symbol": symbol, "intervalo": intervalo, "limit": limit},
        )
        return []

    end_ts = current_ts if current_ts is not None else None
    normalized: Dict[int, Dict[str, float]] = {}
    for candle in fetched:
        timestamp_raw = (
            candle.get("timestamp")
            or candle.get("close_time")
            or candle.get("closeTime")
            or candle.get("open_time")
            or candle.get("openTime")
        )
        try:
            ts = int(float(timestamp_raw)) if timestamp_raw is not None else None
        except (TypeError, ValueError):
            ts = None
        if ts is None:
            continue
        if ts < start_ts:
            continue
        if end_ts is not None and ts >= end_ts:
            continue

        def _safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        normalized[ts] = {
            "symbol": str(symbol).upper(),
            "timestamp": ts,
            "open": _safe_float(candle.get("open")),
            "high": _safe_float(candle.get("high")),
            "low": _safe_float(candle.get("low")),
            "close": _safe_float(candle.get("close")),
            "volume": _safe_float(candle.get("volume")),
            "is_closed": True,
        }

    ordered = [normalized[key] for key in sorted(normalized)]
    if ordered:
        log.debug(
            "backfill_ventana.completed",
            extra={
                "symbol": symbol,
                "intervalo": intervalo,
                "count": len(ordered),
                "start_ts": ordered[0]["timestamp"],
                "end_ts": ordered[-1]["timestamp"],
            },
        )
    return ordered
