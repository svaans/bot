"""Cálculo de umbral adaptativo para entradas.

Este módulo calcula un umbral mínimo para validar señales de entrada. El
resultado se obtiene combinando factores derivados de la volatilidad,
la pendiente y el RSI actuales. Cada componente se calcula por separado
para facilitar la auditoría del proceso.
"""
from __future__ import annotations
import json
import math
from collections import deque
from pathlib import Path
from typing import Deque, Dict, Optional

import pandas as pd
from core.utils.metrics_compat import Counter, Gauge
from indicadores.helpers import get_rsi, get_slope
from indicadores.retornos_volatilidad import (
    retornos_log,
    retornos_simples,
    verificar_consistencia,
    volatilidad_welford,
)
from core.utils.utils import configurar_logger, ESTADO_DIR
log = configurar_logger("adaptador_umbral")
RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CONFIG_CACHE: Dict[str, dict] | None = None
_UMBRAL_SUAVIZADO: Dict[str, float] = {}
# Contador local para saltos de histéresis
_HISTERESIS_SKIPS: Dict[str, int] = {}
_UMBRAL_HISTORICO: Dict[str, Deque[float]] = {}
RUTA_ESTADO = Path(ESTADO_DIR) / "umbral_adaptativo.json"
# Parámetros de suavizado
ALPHA_BASE = 0.3
ALPHA_VOLATILIDAD_ALTA = 0.5
PERCENTIL_VOL_ALTO = 0.9
HISTERESIS = 0.05

UMBRAL_ADAPTATIVO_ACTUAL = Gauge(
    "umbral_adaptativo_actual",
    "Valor actual del umbral adaptativo",
    ["symbol"],
)
UMBRAL_HISTERESIS_SKIPS = Counter(
    "umbral_histeresis_skips_total",
    "Actualizaciones omitidas por la banda de histéresis",
    ["symbol"],
)


def cargar_estado() -> None:
    if not RUTA_ESTADO.exists():
        return
    try:
        with open(RUTA_ESTADO, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        _UMBRAL_SUAVIZADO.update(
            {k: float(v) for k, v in data.get("umbral_suavizado", {}).items()}
        )
        for sym, val in _UMBRAL_SUAVIZADO.items():
            UMBRAL_ADAPTATIVO_ACTUAL.labels(symbol=sym).set(val)
        _HISTERESIS_SKIPS.update(
            {k: int(v) for k, v in data.get("histeresis_skips", {}).items()}
        )
        for sym, count in _HISTERESIS_SKIPS.items():
            if count:
                UMBRAL_HISTERESIS_SKIPS.labels(symbol=sym).inc(count)
    except Exception as e:  # pragma: no cover - logging de carga
        log.warning(f"⚠️ Error cargando estado umbral: {e}")


def guardar_estado() -> None:
    try:
        RUTA_ESTADO.parent.mkdir(parents=True, exist_ok=True)
        with open(RUTA_ESTADO, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "umbral_suavizado": _UMBRAL_SUAVIZADO,
                    "histeresis_skips": _HISTERESIS_SKIPS,
                },
                fh,
                indent=2,
            )
    except Exception as e:  # pragma: no cover - logging de guardado
        log.warning(f"⚠️ Error guardando estado umbral: {e}")


def _cargar_config() -> Dict[str, dict]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        if RUTA_CONFIG.exists():
            with open(RUTA_CONFIG, "r", encoding="utf-8") as fh:
                _CONFIG_CACHE = json.load(fh)
        else:
            _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def calcular_umbral_adaptativo(
    symbol: str, df: pd.DataFrame, contexto: Optional[Dict] = None
) -> float:
    """Devuelve un umbral adaptativo basado en datos técnicos actuales.

    El cálculo parte de los pesos configurados y aplica multiplicadores
    según tres indicadores:

    - **Volatilidad**: cuanto mayor sea, más se incrementa el umbral para
      filtrar entradas en mercados agitados.
    - **Slope**: la pendiente del precio actúa como factor de impulso, tanto
      al alza como a la baja.
    - **RSI**: alejarse de la zona neutra (50) aumenta la exigencia del
      umbral.
    - **Persistencia**: una alta consistencia en señales previas puede
      reducir ligeramente el umbral requerido.
    """
    config = _cargar_config().get(symbol, {})
    base = config.get("peso_minimo_total", 0.5)
    factor = config.get("factor_umbral", 1.0)

    # Cálculo de volatilidad actual y percentil para suavizado adaptativo
    vol_actual: float | None = None
    vol_umbral: float | None = None
    if df is not None and len(df) >= 21 and "close" in df:
        precios = df["close"].tail(200)
        simples = retornos_simples(precios)
        logs = retornos_log(precios)
        if verificar_consistencia(simples, logs) and len(simples) >= 20:
            vol_actual = volatilidad_welford(simples.tail(20))
            vols = simples.rolling(20).std().dropna()
            if len(vols) > 0:
                vol_umbral = vols.quantile(PERCENTIL_VOL_ALTO)

    def _factor_volatilidad() -> float:
        if vol_actual is None:
            return 1.0
        return 1 + vol_actual * 10

    def _factor_slope(valor: float | None) -> float:
        return 1 + abs(valor) if valor is not None else 1.0

    def _factor_rsi(valor: float | None) -> float:
        return 1 + abs(valor - 50) / 100 if valor is not None else 1.0
    
    def _factor_persistencia(valor: float | None) -> float:
        """Reduce el umbral en función de la persistencia acumulada."""
        if valor is None:
            return 1.0
        return 1 - min(valor * 0.1, 0.3)

    rsi_val = (
        contexto["rsi"]
        if (contexto and "rsi" in contexto and contexto["rsi"] is not None)
        else get_rsi(df)
    )
    slope_val = (
        contexto["slope"]
        if (contexto and "slope" in contexto and contexto["slope"] is not None)
        else get_slope(df)
    )
    if isinstance(rsi_val, pd.Series):
        rsi_val = rsi_val.iloc[-1]
    if isinstance(slope_val, pd.Series):
        slope_val = slope_val.iloc[-1]
    pers_val = contexto.get("persistencia") if contexto else None

    umbral = base * factor
    umbral *= _factor_volatilidad()
    umbral *= _factor_slope(slope_val)
    umbral *= _factor_rsi(rsi_val)
    umbral *= _factor_persistencia(pers_val)
    umbral = max(1.0, umbral)
    if pd.isna(umbral) or not math.isfinite(umbral):
        return 1.0
    
    alpha = ALPHA_BASE
    if vol_actual is not None and vol_umbral is not None and vol_actual > vol_umbral:
        alpha = ALPHA_VOLATILIDAD_ALTA
    previo = _UMBRAL_SUAVIZADO.get(symbol)
    if previo is None:
        suavizado = umbral
    else:
        suavizado = previo + alpha * (umbral - previo)
    if pd.isna(suavizado) or not math.isfinite(suavizado):
        return 1.0
    historial = _UMBRAL_HISTORICO.setdefault(symbol, deque(maxlen=20))
    histeresis_actual = HISTERESIS
    if len(historial) >= 2:
        variacion = sum(
            abs(historial[i] - historial[i - 1]) for i in range(1, len(historial))
        ) / (len(historial) - 1)
        if variacion > HISTERESIS:
            histeresis_actual = HISTERESIS / 2

    if previo is not None and abs(suavizado - previo) < histeresis_actual:
        _HISTERESIS_SKIPS[symbol] = _HISTERESIS_SKIPS.get(symbol, 0) + 1
        UMBRAL_HISTERESIS_SKIPS.labels(symbol=symbol).inc()
        UMBRAL_ADAPTATIVO_ACTUAL.labels(symbol=symbol).set(previo)
        resultado = round(previo, 2)
    else:
        _UMBRAL_SUAVIZADO[symbol] = suavizado
        UMBRAL_ADAPTATIVO_ACTUAL.labels(symbol=symbol).set(suavizado)
        resultado = round(suavizado, 2)
    historial.append(resultado)
    return resultado


def calcular_umbral_salida_adaptativo(
    symbol: str, config: Dict | None = None, contexto: Optional[Dict] = None
) -> float:
    """Calcula un umbral dinámico para salidas basado en contexto de mercado."""
    if config is None:
        config = {}
    base = config.get("umbral_salida_base", 1.5)
    volatilidad = contexto.get("volatilidad", 1.0) if contexto else 1.0
    tendencia = contexto.get("tendencia", "lateral") if contexto else "lateral"
    factor_tend = 1.2 if tendencia in ["alcista", "bajista"] else 1.0
    umbral = base * volatilidad * factor_tend
    log.debug(
        f"[{symbol}] Umbral salida adaptativo: {umbral:.2f} | Base: {base} | Vol: {volatilidad:.3f} | Tend: {tendencia}"
    )
    return umbral
