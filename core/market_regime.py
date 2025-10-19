"""Detección de régimen de mercado con confirmaciones multi-horizonte."""

from __future__ import annotations

from dataclasses import dataclass
from math import isclose
from typing import Dict, Optional

import pandas as pd

from indicadores.helpers import get_atr
from core.utils.market_utils import calcular_slope_pct

ALTA_VOL_THRESHOLD = 0.02
SLOPE_UPPER_THRESHOLD = 0.003
SLOPE_LOWER_THRESHOLD = 0.001
MIN_EMA_SPREAD = 0.001
ATR_TREND_THRESHOLD = 0.012
ATR_RANGE_THRESHOLD = 0.006
ATR_CONFIRM_PERIOD = 14


@dataclass(frozen=True)
class RegimeThresholds:
    """Conjunto de umbrales configurables para clasificación de régimen."""

    alta_volatilidad: float = ALTA_VOL_THRESHOLD
    slope_upper: float = SLOPE_UPPER_THRESHOLD
    slope_lower: float = SLOPE_LOWER_THRESHOLD
    min_ema_spread: float = MIN_EMA_SPREAD
    atr_trend: float = ATR_TREND_THRESHOLD
    atr_range: float = ATR_RANGE_THRESHOLD


@dataclass
class _RegimeState:
    """Mantiene el régimen previo para aplicar histéresis por símbolo."""

    last_regime: str = "lateral"
    last_slope: float = 0.0
    last_direction: int = 0


_REGIME_STATE: Dict[str, _RegimeState] = {}


def medir_volatilidad(df: pd.DataFrame, periodo: int=14) ->float:
    """Devuelve la volatilidad relativa usando ATR."""
    atr = get_atr(df, periodo)
    if atr is None:
        return 0.0
    cierre = float(df['close'].iloc[-1]) if 'close' in df else 0.0
    if isclose(cierre, 0.0, rel_tol=1e-12, abs_tol=1e-12):
        return 0.0
    return float(atr) / cierre


def pendiente_medias(df: pd.DataFrame, ventana: int=30) ->float:
    """Calcula la pendiente porcentual de una media móvil."""
    if 'close' not in df or len(df) < ventana + 5:
        return 0.0
    sma = df['close'].rolling(window=ventana).mean().dropna()
    if len(sma) < 2:
        return 0.0
    return calcular_slope_pct(sma)



def _extract_symbol(df: pd.DataFrame) -> str:
    """Obtiene el símbolo asociado al ``DataFrame`` o devuelve ``GLOBAL``."""

    symbol = getattr(df, "symbol", None)
    if isinstance(symbol, str) and symbol:
        return symbol.upper()
    symbol = df.attrs.get("symbol")
    if isinstance(symbol, str) and symbol:
        return symbol.upper()
    return "GLOBAL"


def _get_state(symbol: str) -> _RegimeState:
    state = _REGIME_STATE.get(symbol)
    if state is None:
        state = _RegimeState()
        _REGIME_STATE[symbol] = state
    return state


def reset_regimen_cache(symbol: Optional[str] = None) -> None:
    """Resetea el estado almacenado para histéresis en los símbolos."""

    if symbol is None:
        _REGIME_STATE.clear()
        return
    _REGIME_STATE.pop(symbol.upper(), None)


def _extract_implied_vol(df: pd.DataFrame) -> float:
    """Busca volatilidad implícita en atributos o columnas conocidas."""

    candidatos = (
        "implied_volatility",
        "impliedVolatility",
        "implied_vol",
        "volatilidad_implicita",
    )
    for key in candidatos:
        valor = df.attrs.get(key)
        if valor is None and key in df.columns:
            serie = df[key].dropna()
            valor = float(serie.iloc[-1]) if not serie.empty else None
        if valor is None:
            continue
        try:
            return max(0.0, float(valor))
        except (TypeError, ValueError):
            continue
    return 0.0


def _infer_datetime_index(df: pd.DataFrame) -> Optional[pd.DatetimeIndex]:
    """Construye un índice temporal si es posible."""

    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        return df.index
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.tz_localize("UTC")
    if "timestamp" not in df:
        return None
    serie = df["timestamp"].dropna()
    if serie.empty:
        return None
    try:
        maximo = float(serie.iloc[-1])
    except (TypeError, ValueError):
        return None
    unit = "ms" if maximo > 1e11 else "s"
    try:
        return pd.to_datetime(df["timestamp"], unit=unit, utc=True)
    except (TypeError, ValueError):
        return None


def _resample(df: pd.DataFrame, rule: str) -> Optional[pd.DataFrame]:
    """Resamplea el ``DataFrame`` con OHLCV si hay información temporal."""

    index = _infer_datetime_index(df)
    if index is None or len(df) == 0:
        return None
    base = df.loc[:, [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]].copy()
    if base.empty:
        return None
    base.index = index
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    result = base.resample(rule).agg(agg).dropna()
    return result if not result.empty else None


def _ema_confirmation(
    df: pd.DataFrame, slope_pct: float, thresholds: RegimeThresholds
) -> Optional[bool]:
    """Confirma tendencia con EMAs 50/200 en 1H si hay datos suficientes."""

    resampled = _resample(df, "1H")
    if resampled is None or "close" not in resampled:
        return None
    cierres = resampled["close"].dropna()
    if len(cierres) < 200:
        return None
    ema50 = cierres.ewm(span=50, adjust=False).mean()
    ema200 = cierres.ewm(span=200, adjust=False).mean()
    ema50_last = float(ema50.iloc[-1])
    ema200_last = float(ema200.iloc[-1])
    if isclose(ema200_last, 0.0, abs_tol=1e-12):
        return None
    spread = (ema50_last - ema200_last) / abs(ema200_last)
    if abs(spread) < thresholds.min_ema_spread:
        return False
    direction = 1 if slope_pct >= 0 else -1
    if direction >= 0:
        return ema50_last > ema200_last
    return ema50_last < ema200_last


def _atr_confirmation(
    df: pd.DataFrame, thresholds: RegimeThresholds
) -> Optional[bool]:
    """Utiliza ATR en 15m para validar si el mercado está dinámico."""

    resampled = _resample(df, "15T")
    if resampled is None:
        return None
    atr = get_atr(resampled, ATR_CONFIRM_PERIOD)
    if atr is None:
        return None
    cierre = float(resampled["close"].iloc[-1]) if "close" in resampled else 0.0
    if isclose(cierre, 0.0, abs_tol=1e-12):
        return None
    ratio = float(atr) / cierre
    if ratio >= thresholds.atr_trend:
        return True
    if ratio <= thresholds.atr_range:
        return False
    return None


def _multi_horizon_confirmation(
    df: pd.DataFrame, slope_pct: float, thresholds: RegimeThresholds
) -> Optional[bool]:
    """Combina las confirmaciones de EMA 1H y ATR 15m."""

    confirmaciones = []
    ema_conf = _ema_confirmation(df, slope_pct, thresholds)
    if ema_conf is not None:
        confirmaciones.append(ema_conf)
    atr_conf = _atr_confirmation(df, thresholds)
    if atr_conf is not None:
        confirmaciones.append(atr_conf)
    if not confirmaciones:
        return None
    if not all(confirmaciones):
        return False
    return True


def _resolver_regimen(
    state: _RegimeState,
    slope_pct: float,
    tendencia_confirmada: Optional[bool],
    thresholds: RegimeThresholds,
) -> str:
    """Aplica histéresis sobre el ``slope`` para clasificar el régimen."""

    abs_slope = abs(slope_pct)
    if tendencia_confirmada is False:
        return "lateral"
    if state.last_regime == "tendencial":
        if abs_slope <= thresholds.slope_lower:
            return "lateral"
        return "tendencial"
    if abs_slope >= thresholds.slope_upper and tendencia_confirmada in (None, True):
        return "tendencial"
    return "lateral"


def detectar_regimen(
    df: pd.DataFrame, *, thresholds: RegimeThresholds | None = None
) -> str:
    """Clasifica el régimen de mercado con histéresis y confirmaciones."""

    symbol = _extract_symbol(df)
    state = _get_state(symbol)
    params = thresholds or RegimeThresholds()
    vol_realizada = medir_volatilidad(df)
    vol_impl = _extract_implied_vol(df)
    atr_ratio = _atr_ratio_actual(df)
    vol_total = max(vol_realizada, vol_impl, atr_ratio)
    slope_pct = pendiente_medias(df)
    if vol_total >= params.alta_volatilidad:
        state.last_regime = "alta_volatilidad"
        state.last_slope = slope_pct
        state.last_direction = 1 if slope_pct > 0 else -1 if slope_pct < 0 else 0
        return "alta_volatilidad"
    confirmacion = _multi_horizon_confirmation(df, slope_pct, params)
    nuevo_regimen = _resolver_regimen(state, slope_pct, confirmacion, params)
    state.last_regime = nuevo_regimen
    state.last_slope = slope_pct
    state.last_direction = 1 if slope_pct > 0 else -1 if slope_pct < 0 else 0
    return nuevo_regimen


def _atr_ratio_actual(df: pd.DataFrame) -> float:
    """Calcula un ratio de ATR en 15m reutilizable para volatilidad agregada."""

    resampled = _resample(df, "15T")
    if resampled is None:
        return 0.0
    atr = get_atr(resampled, ATR_CONFIRM_PERIOD)
    if atr is None:
        return 0.0
    cierre = float(resampled["close"].iloc[-1]) if "close" in resampled else 0.0
    if isclose(cierre, 0.0, abs_tol=1e-12):
        return 0.0
    return float(atr) / cierre
