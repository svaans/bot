"""
verificar_entrada_lite — Evaluación de entrada mínima, robusta y desacoplada.

Objetivo
--------
- Mantener la lógica esencial para decidir una posible entrada sin acoplarse
  a subsistemas pesados (métricas, notificaciones, aprendizaje, etc.).
- Ser tolerante a contratos cambiantes en utilidades del proyecto (p. ej.
  `verificar_integridad_datos` retornando df o (ok, df)).
- Respetar límites de tiempo por símbolo (budget/timeout blando).

Entradas clave
--------------
async verificar_entrada_lite(trader, symbol: str, df: pd.DataFrame, estado) -> dict | None
  - `trader` debe exponer como mínimo:
      - config.intervalo_velas (str)
      - config.timeout_evaluar_condiciones (float)
      - config.timeout_evaluar_condiciones_por_symbol (dict[str,float]) (opcional)
      - config.version (opcional)
      - config.max_perdidas_diarias (opcional)
      - capital_manager.info_mercado(symbol) (opcional) -> obj con tick_size/step_size
      - engine.evaluar_entrada(symbol, df, tendencia, config, pesos_symbol) -> dict
      - persistencia.actualizar(symbol, estrategias)
      - persistencia.es_persistente(symbol, estrategia) -> bool
      - persistencia.peso_extra (float)
      - pesos_por_simbolo: dict[str, dict[str, float]]
      - orders.ordenes: dict[str, orden]
      - historial_cierres: dict[str, dict]
  - `estado` debe tener: buffer (deque), estrategias_buffer (deque), df (pd.DataFrame opcional)

Notas
-----
- Este módulo intenta no depender directamente de trackers/metricas; en su lugar
  emite códigos de filtro vía `on_event` si se provee (callback opcional).
- Donde el proyecto original usa muchas dependencias, aquí se encapsulan en
  pequeñas funciones auxiliares con `try/except` para evitar fallos duros.
"""
from __future__ import annotations
import asyncio
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

import pandas as pd

# Imports tolerantes a fallos (se usan si están disponibles)
try:
    from core.utils.utils import (
        verificar_integridad_datos,
        intervalo_a_segundos,
        timestamp_alineado,
    )
except Exception:  # pragma: no cover
    def verificar_integridad_datos(df: pd.DataFrame):
        return True, df
    def intervalo_a_segundos(x: str) -> int:
        m = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}
        return m.get(x, 60)
    def timestamp_alineado(ts: int, intervalo: str) -> bool:
        base = intervalo_a_segundos(intervalo) * 1000
        return ts % base == 0

try:
    from core.utils import safe_resample, configurar_logger
except Exception:  # pragma: no cover
    def safe_resample(df: pd.DataFrame, rule: str) -> pd.DataFrame:
        return df.resample(rule)
    def configurar_logger(name: str):
        import logging
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

try:
    from core.strategies.tendencia import obtener_tendencia
except Exception:  # pragma: no cover
    def obtener_tendencia(symbol: str, df: pd.DataFrame) -> str | None:
        return None

try:
    from core.estrategias import filtrar_por_direccion
except Exception:  # pragma: no cover
    def filtrar_por_direccion(estrategias: dict, direccion: str):
        return estrategias, []

try:
    from core.adaptador_umbral import calcular_umbral_adaptativo
except Exception:  # pragma: no cover
    def calcular_umbral_adaptativo(symbol: str, df: pd.DataFrame, ctx: dict) -> float:
        return 1.0

try:
    from core.adaptador_dinamico import calcular_tp_sl_adaptativos
except Exception:  # pragma: no cover
    def calcular_tp_sl_adaptativos(symbol: str, df: pd.DataFrame, config: dict, capital: float, precio: float):
        return (precio * 0.99, precio * 1.02)

try:
    from core.risk.validators import validate_levels, LevelValidationError
except Exception:  # pragma: no cover
    class LevelValidationError(Exception):
        def __init__(self, reason: str, context: dict | None = None):
            super().__init__(reason)
            self.reason = reason
            self.context = context or {}
    def validate_levels(direccion, precio, sl, tp, min_dist_pct, tick, step):
        return precio, sl, tp

try:
    from core.strategies.evaluador_tecnico import (
        evaluar_puntaje_tecnico,
        cargar_pesos_tecnicos,
    )
except Exception:  # pragma: no cover
    async def evaluar_puntaje_tecnico(symbol, df, precio, sl, tp):
        return {"score_total": 1.0, "detalles": {}}
    async def cargar_pesos_tecnicos(symbol):
        return {"base": 1.0}

try:
    from core.config_manager.dinamica import adaptar_configuracion
except Exception:  # pragma: no cover
    def adaptar_configuracion(symbol: str, df: pd.DataFrame, cfg: dict) -> dict:
        return dict(cfg)

try:
    from core.scoring import DecisionTrace, DecisionReason
except Exception:  # pragma: no cover
    class DecisionReason:
        BELOW_THRESHOLD = "BELOW_THRESHOLD"
    class DecisionTrace:
        def __init__(self, a, b, c, d): ...
        def to_json(self): return "{}"

try:  # memo indicadores suaves
    from indicators.helpers import get_rsi, get_momentum
except Exception:  # pragma: no cover
    def get_rsi(df: pd.DataFrame):
        return None
    def get_momentum(df: pd.DataFrame):
        return None

log = configurar_logger("verificar_entrada_lite")
UTC = timezone.utc
MAX_BACKFILL_CANDLES = 100
MIN_BUFFER_CANDLES = int(os.getenv("MIN_BUFFER_CANDLES", "30"))

_indicador_cache: dict[tuple[str, int], tuple[float | None, float | None]] = {}


def _emit(on_event: Optional[Callable[[str, dict], None]], evt: str, data: dict) -> None:
    if on_event:
        try:
            on_event(evt, data)
        except Exception:
            pass


def _memo_indicadores(symbol: str, df: pd.DataFrame) -> tuple[float | None, float | None]:
    ts = int(df["timestamp"].iloc[-1]) if not df.empty else 0
    key = (symbol, ts)
    cached = _indicador_cache.get(key)
    if cached:
        return cached
    rsi = get_rsi(df)
    mom = get_momentum(df)
    r = rsi if isinstance(rsi, (int, float)) else None
    m = mom if isinstance(mom, (int, float)) else None
    _indicador_cache[key] = (r, m)
    return _indicador_cache[key]


def _buffer_ready(estado, intervalo: str) -> tuple[bool, str]:
    ts = [c.get("timestamp") for c in estado.buffer if c.get("timestamp") is not None]
    if len(ts) < MIN_BUFFER_CANDLES:
        return False, "prebuffer"
    if any(not timestamp_alineado(t, intervalo) for t in ts):
        return False, "misaligned"
    if any(ts[i] <= ts[i - 1] for i in range(1, len(ts))):
        return False, "out_of_order"
    return True, ""


def _sanear_df(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    try:
        res = verificar_integridad_datos(df)
        if isinstance(res, tuple):
            ok, reparado = res
        else:
            ok, reparado = True, res
    except Exception as e:
        log.warning(f"⚠️ Error en verificar_integridad_datos: {e}")
        return False, df
    if reparado is None or getattr(reparado, "empty", False):
        return False, df
    return ok, reparado


def _resolve_min_dist_pct(symbol: str, trader_config, symbol_config: dict | None) -> float:
    base = getattr(trader_config, "min_dist_pct", 0.0005)
    valor = None
    if isinstance(symbol_config, dict):
        raw = symbol_config.get("distancia_minima_pct", symbol_config.get("min_distancia_pct"))
        if raw is not None:
            try:
                valor = float(raw)
            except (TypeError, ValueError):
                valor = None
    overrides = getattr(trader_config, "min_dist_pct_overrides", {})
    if isinstance(overrides, dict):
        override = overrides.get(symbol) or overrides.get(symbol.replace("/", ""))
        if override is not None:
            try:
                valor = float(override)
            except (TypeError, ValueError):
                pass
    if valor is None:
        valor = base
    try:
        valor = float(valor)
    except (TypeError, ValueError):
        valor = float(base)
    return max(valor, 1e-6)


def _tendencia_principal(valores: list[str | None]) -> tuple[str | None, float]:
    vals = [t for t in valores if t]
    if not vals:
        return None, 0.0
    tendencia = max(set(vals), key=vals.count)
    return tendencia, vals.count(tendencia) / len(vals)


def _validar_marcos(symbol: str, df: pd.DataFrame, df5: pd.DataFrame | None, df1h: pd.DataFrame | None, df1d: pd.DataFrame | None, config: dict) -> bool:
    umbral_micro = config.get("umbral_confirmacion_micro", 0.6)
    umbral_macro = config.get("umbral_confirmacion_macro", 0.6)
    micro = []
    macro = []
    try:
        micro = [obtener_tendencia(symbol, df), obtener_tendencia(symbol, df5)] if df5 is not None else [obtener_tendencia(symbol, df)]
        macro = [obtener_tendencia(symbol, df1h), obtener_tendencia(symbol, df1d)]
    except Exception:
        return True
    mdir, mrat = _tendencia_principal(micro)
    Mdir, Mrat = _tendencia_principal(macro)
    if mdir and Mdir and mrat >= umbral_micro and Mrat >= umbral_macro and mdir != Mdir:
        return False
    return True


async def verificar_entrada_lite(
    trader: Any,
    symbol: str,
    df: pd.DataFrame,
    estado: Any,
    *,
    on_event: Optional[Callable[[str, dict], None]] = None,
) -> dict | None:
    """Evalúa condiciones de entrada y devuelve un dict con la propuesta o None."""
    timeout_cfg = getattr(trader.config, "timeout_evaluar_condiciones_por_symbol", {}).get(
        symbol, getattr(trader.config, "timeout_evaluar_condiciones", 2.0)
    )
    deadline = time.perf_counter() + float(timeout_cfg)
    def _budget_exceeded() -> bool:
        return time.perf_counter() > deadline

    # 1) Buffer mínimo
    ok_buf, motivo = _buffer_ready(estado, trader.config.intervalo_velas)
    if not ok_buf:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": motivo})
        return None

    # 2) Normalización / integridad
    if df is None or df.empty:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "datos_invalidos"})
        return None
    df = df.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    ok, df = _sanear_df(df)
    if not ok:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "datos_invalidos"})
        return None

    if _budget_exceeded():
        _emit(on_event, "timeout", {"symbol": symbol, "fase": "integridad"})
        return None

    # 3) Adaptar configuración + tendencia
    config = adaptar_configuracion(symbol, df, getattr(trader, "config_por_simbolo", {}).get(symbol, {}))
    getattr(trader, "config_por_simbolo", {}).update({symbol: config})

    tendencia = obtener_tendencia(symbol, df)

    # 4) Marcos superiores
    df_idx = df.set_index(pd.to_datetime(df["timestamp"], unit="ms"))
    df_5m = safe_resample(df_idx, "5min").last().dropna() if len(df_idx) >= 5 else None
    df_1h = safe_resample(df_idx, "1h").last().dropna() if len(df_idx) >= 60 else None
    df_1d = safe_resample(df_idx, "1d").last().dropna() if len(df_idx) >= 1440 else None
    if not _validar_marcos(symbol, df, df_5m, df_1h, df_1d, config):
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "marcos_temporales"})
        return None

    # 5) Engine de estrategias
    if _budget_exceeded():
        _emit(on_event, "timeout", {"symbol": symbol, "fase": "pre_engine"})
        return None
    engine_eval = await trader.engine.evaluar_entrada(
        symbol,
        df,
        tendencia=tendencia,
        config={**config, "contradicciones_bloquean_entrada": getattr(trader, "contradicciones_bloquean_entrada", True), "usar_score_tecnico": getattr(trader, "usar_score_tecnico", True)},
        pesos_symbol=getattr(trader, "pesos_por_simbolo", {}).get(symbol, {}),
    )
    estrategias = engine_eval.get("estrategias_activas", {})
    if not estrategias:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "sin_estrategias"})
        return None

    # Persistencia de estrategias
    if getattr(estado, "estrategias_buffer", None):
        try:
            estado.estrategias_buffer[-1] = estrategias
        except Exception:
            pass
    if hasattr(trader, "persistencia"):
        try:
            trader.persistencia.actualizar(symbol, estrategias)
        except Exception:
            pass

    # 6) Umbral y puntaje base
    persistencia_score = 1.0
    if hasattr(trader, "persistencia") and hasattr(trader.persistencia, "peso_extra"):
        try:
            hist = list(getattr(estado, "estrategias_buffer", []))[-100:]
            from core.data import coincidencia_parcial  # import tardío
            persistencia_score = coincidencia_parcial(hist, getattr(trader, "pesos_por_simbolo", {}).get(symbol, {}), ventanas=5)
        except Exception:
            persistencia_score = 1.0
    contexto_umbral = {"rsi": engine_eval.get("rsi"), "slope": engine_eval.get("slope"), "persistencia": persistencia_score}
    umbral = calcular_umbral_adaptativo(symbol, df, contexto_umbral)

    direccion = "short" if tendencia == "bajista" else "long"
    estrategias_persistentes = {e: True for e, activo in estrategias.items() if activo and getattr(trader, "persistencia", None) and trader.persistencia.es_persistente(symbol, e)}
    estrategias_filtradas, incoherentes = filtrar_por_direccion(estrategias_persistentes, direccion)
    penalizacion = 0.05 * len(incoherentes) if incoherentes else 0.0

    pesos = getattr(trader, "pesos_por_simbolo", {}).get(symbol, {})
    puntaje = sum(pesos.get(e, 0) for e in estrategias_filtradas) + getattr(getattr(trader, "persistencia", None), "peso_extra", 0.0) * len(estrategias_filtradas) - penalizacion

    if _budget_exceeded():
        _emit(on_event, "timeout", {"symbol": symbol, "fase": "post_engine"})
        return None

    # 7) RSI/momentum con cache de respaldo
    rsi_cache, mom_cache = _memo_indicadores(symbol, df)
    rsi = engine_eval.get("rsi")
    if isinstance(rsi, pd.Series):
        rsi = float(rsi.iloc[-1])
    if rsi is None:
        rsi = rsi_cache
    momentum = engine_eval.get("momentum")
    if isinstance(momentum, pd.Series):
        momentum = float(momentum.iloc[-1])
    if momentum is None:
        momentum = mom_cache
    if rsi is None or momentum is None:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "datos_invalidos"})
        return None

    # 8) Diversidad simple (si lo deseas, reemplaza por tu validador)
    peso_total = sum(pesos.get(e, 0) for e in estrategias_filtradas)
    peso_min_total = float(config.get("peso_minimo_total", 0.5) or 0.5)
    diversidad_min = int(config.get("diversidad_minima", 2) or 2)
    diversidad_ok = (len(estrategias_filtradas) >= diversidad_min) or (peso_total >= peso_min_total)

    if not diversidad_ok and not bool(config.get("modo_agresivo", False)):
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "diversidad"})
        return None

    # 9) SL/TP adaptativos + validación de niveles
    precio = float(df["close"].iloc[-1])
    sl, tp = calcular_tp_sl_adaptativos(symbol, df, config, getattr(trader, "capital_por_simbolo", {}).get(symbol, 0.0), precio)

    tick_size = 0.0
    step_size = 0.0
    if getattr(trader, "capital_manager", None):
        try:
            market = await trader.capital_manager.info_mercado(symbol)
            tick_size = getattr(market, "tick_size", 0.0) or 0.0
            step_size = getattr(market, "step_size", 0.0) or 0.0
        except Exception:
            pass

    min_dist_pct = _resolve_min_dist_pct(symbol, trader.config, config)
    try:
        precio, sl, tp = validate_levels(direccion, precio, sl, tp, min_dist_pct, tick_size, step_size)
    except LevelValidationError as exc:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "sl_tp", "detalle": exc.reason})
        return None

    sl_diff = abs(precio - sl)
    tp_diff = abs(tp - precio)
    ratio_minimo = max(1.0, float(config.get("ratio_minimo_beneficio", 1.3) or 1.3))
    ratio_final = tp_diff / sl_diff if sl_diff > 0 else math.inf
    if not math.isfinite(ratio_final) or ratio_final < ratio_minimo:
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "sl_tp_ratio", "ratio": ratio_final})
        return None

    # 10) Score técnico (normalizado vs umbral)
    eval_tecnica = await evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = float(eval_tecnica.get("score_total", 0.0))
    pesos_simbolo = await cargar_pesos_tecnicos(symbol)
    score_max = sum(float(v) for v in pesos_simbolo.values()) or 1.0
    score_normalizado = float(eval_tecnica.get("score_normalizado", score_total / score_max))
    umbral_normalizado = (umbral / score_max) if score_max else umbral
    if score_normalizado < umbral_normalizado:
        trace = DecisionTrace(score_normalizado, umbral_normalizado, DecisionReason.BELOW_THRESHOLD, eval_tecnica.get("detalles", {}))
        log.info(f"[{symbol}] Trace: {trace.to_json()}")
        _emit(on_event, "filtro", {"symbol": symbol, "razon": "score_tecnico"})
        return None

    # Listo
    candle_ts = int(df["timestamp"].iloc[-1])
    version = getattr(trader.config, "version", "v1")
    return {
        "symbol": symbol,
        "precio": precio,
        "sl": sl,
        "tp": tp,
        "tick_size": tick_size,
        "step_size": step_size,
        "min_dist_pct": min_dist_pct,
        "estrategias": estrategias_filtradas,
        "puntaje": puntaje,
        "umbral": umbral,
        "tendencia": tendencia,
        "direccion": "short" if tendencia == "bajista" else "long",
        "candle_close_ts": candle_ts,
        "strategy_version": version,
        "score_tecnico": score_normalizado,
        "detalles_tecnicos": eval_tecnica.get("detalles", {}),
    }


