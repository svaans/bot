# core/procesar_vela.py
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, Optional, Tuple, Union

import pandas as pd

from core.utils.logger import configurar_logger
from prometheus_client import Gauge

from core.metrics_helpers import safe_inc, safe_set
from core.utils.metrics_compat import Counter, Histogram

try:  # pragma: no cover - métricas opcionales
    from core.metrics import (
        BUFFER_SIZE_V2,
        CANDLES_PROCESSED_TOTAL,
        ENTRADAS_RECHAZADAS_V2,
        EVAL_INTENTOS_TOTAL,
        LAST_BAR_AGE,
        WARMUP_RESTANTE,
    )
except Exception:  # pragma: no cover - fallback si core.metrics no está disponible
    class _NullMetric:
        def labels(self, *_args: Any, **_kwargs: Any) -> "_NullMetric":  # type: ignore[name-defined]
            return self

        def set(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    WARMUP_RESTANTE = _NullMetric()  # type: ignore[assignment]
    LAST_BAR_AGE = _NullMetric()  # type: ignore[assignment]
    BUFFER_SIZE_V2 = _NullMetric()  # type: ignore[assignment]
    ENTRADAS_RECHAZADAS_V2 = Counter(
        "procesar_vela_entradas_rechazadas_total_v2",
        "Entradas rechazadas tras validaciones finales",
        ["symbol", "timeframe", "reason"],
    )
    CANDLES_PROCESSED_TOTAL = Counter(
        "candles_processed_total",
        "Velas cerradas procesadas por símbolo y timeframe",
        ["symbol", "timeframe"],
    )
    EVAL_INTENTOS_TOTAL = Counter(
        "eval_intentos_total",
        "Intentos de evaluación clasificados por etapa",
        ["symbol", "timeframe", "etapa"],
    )

try:  # Preferir constante compartida para coherencia con warmup
    from core.data.bootstrap import MIN_BARS as _DEFAULT_MIN_BARS
except Exception:  # pragma: no cover - fallback cuando no existe el módulo
    _DEFAULT_MIN_BARS = int(os.getenv("MIN_BARS", "400"))

DEFAULT_MIN_BARS = _DEFAULT_MIN_BARS

# ──────────────────────────────────────────────────────────────────────────────
# Métricas (compatibles aunque no haya prometheus_client)
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULT_LATENCY_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5)

EVAL_LATENCY = Histogram(
    "procesar_vela_eval_latency_seconds",
    "Latencia total de procesar_vela (parseo + buffers + estrategia)",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
PARSE_LATENCY = Histogram(
    "procesar_vela_parse_latency_seconds",
    "Latencia de normalización y sanitización de velas",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
GATING_LATENCY = Histogram(
    "procesar_vela_gating_latency_seconds",
    "Latencia de buffers y validaciones previas a la estrategia",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
STRATEGY_LATENCY = Histogram(
    "procesar_vela_strategy_latency_seconds",
    "Latencia de evaluación de estrategias y scoring",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
HANDLER_EXCEPTIONS = Counter(
    "procesar_vela_exceptions_total",
    "Excepciones no controladas en procesar_vela",
)
CANDLES_IGNORADAS = Counter(
    "procesar_vela_ignorada_total",
    "Velas ignoradas por validación previa o falta de datos",
    ["reason"],
)
ENTRADAS_CANDIDATAS = Counter(
    "procesar_vela_entradas_candidatas_total",
    "Entradas candidatas generadas por símbolo",
    ["symbol", "side"],
)
ENTRADAS_ABIERTAS = Counter(
    "procesar_vela_entradas_abiertas_total",
    "Entradas abiertas (OrderManager)",
    ["symbol", "side"],
)

log = configurar_logger("procesar_vela")

# ──────────────────────────────────────────────────────────────────────────────
# Utilidades y estructuras internas
# ──────────────────────────────────────────────────────────────────────────────

COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def _attach_timeframe(df: pd.DataFrame, timeframe: Optional[str]) -> None:
    """Adjunta el timeframe al DataFrame sin contaminar columnas."""

    if not timeframe:
        return

    try:
        object.__setattr__(df, "tf", timeframe)
    except Exception:
        with contextlib.suppress(Exception):
            df.attrs["tf"] = timeframe


def _resolve_min_bars(trader: Any, default: int = DEFAULT_MIN_BARS) -> int:
    """Determina el mínimo de velas requerido para evaluar estrategias."""

    candidatos: list[Any] = []

    for attr in ("min_bars", "min_buffer_candles", "min_velas"):
        candidatos.append(getattr(trader, attr, None))

    cfg = getattr(trader, "config", None)
    if cfg is not None:
        for attr in ("min_bars", "min_buffer_candles", "min_velas", "min_velas_evaluacion"):
            candidatos.append(getattr(cfg, attr, None))

    for valor in candidatos:
        if valor is None:
            continue
        try:
            entero = int(valor)
        except (TypeError, ValueError):
            continue
        if entero > 0:
            return entero

    return default

def _is_num(x: Any) -> bool:
    try:
        return not (x is None or isinstance(x, bool) or math.isnan(float(x)) or math.isinf(float(x)))
    except Exception:
        return False


def _validar_candle(c: dict) -> Tuple[bool, str]:
    # Campos mínimos
    for k in COLUMNS:
        if k not in c:
            return False, f"missing_{k}"
        if not _is_num(c[k]):
            return False, f"nan_{k}"
    # Orden temporal
    if int(c["timestamp"]) <= 0:
        return False, "bad_ts"
    # OHLC mínimos
    o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    if not (l <= o <= h and l <= cl <= h and h >= l and o > 0 and cl > 0):
        return False, "bad_ohlc"
    return True, ""


def _hash_buffer(items: Deque[dict]) -> Tuple[int, int]:
    """Pequeño hash barato: (len, last_ts)."""
    if not items:
        return (0, 0)
    return (len(items), int(items[-1].get("timestamp", 0)))


def _mark_skip(
    target: dict,
    reason: str,
    details: Optional[dict] = None,
    *,
    gate: Optional[str] = None,
    score: Any | None = None,
) -> None:
    """Adjunta metadatos de skip al diccionario de la vela."""

    if not isinstance(target, dict):
        return
    
    target["_df_skip_reason"] = reason
    payload: dict[str, Any] | None = None
    if isinstance(details, dict):
        payload = dict(details)

    if payload is None:
        payload = {}

    gate_value = gate or payload.get("gate")
    if gate_value is None and reason:
        gate_value = reason
    if gate_value is not None:
        payload["gate"] = str(gate_value)

    score_value: Any | None = score
    if score_value is None and "score" in payload:
        score_value = payload.get("score")
    if score_value is not None:
        if _is_num(score_value):
            payload["score"] = float(score_value)
        else:
            payload["score"] = score_value

    if payload:
        target["_df_skip_details"] = payload
    else:
        target.pop("_df_skip_details", None)

    symbol = str(target.get("symbol") or "").upper()
    ts_candidate = (
        target.get("timestamp")
        or target.get("close_time")
        or target.get("open_time")
        or target.get("event_time")
    )
    trace_id: str | None = None
    try:
        if symbol and ts_candidate is not None:
            ts_int = int(float(ts_candidate))
            base = f"{symbol}:{ts_int}".encode("utf-8")
            trace_id = hashlib.blake2b(base, digest_size=12).hexdigest()
    except (TypeError, ValueError):  # pragma: no cover - valores inesperados
        trace_id = None

    if trace_id:
        target["_df_trace_id"] = trace_id
    else:
        target.pop("_df_trace_id", None)


def _normalize_timestamp(value: Any) -> Optional[float]:
    """Normaliza timestamps en segundos (acepta ms)."""

    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric >= 1e11:
        return numeric / 1000.0
    return numeric


# ──────────────────────────────────────────────────────────────────────────────
# Helpers solicitados por tests: spread
# ──────────────────────────────────────────────────────────────────────────────

def _approximate_spread(
    snapshot_or_bid: Union[dict, float, int, None],
    ask: Optional[float] = None,
) -> Optional[float]:
    """
    Modos:
      - _approximate_spread(snapshot: dict) -> ratio relativo (spread/close).
        Si 'close' <= 0 pero high>=low, devuelve 0.0 (comportamiento esperado por tests).
      - _approximate_spread(bid: float, ask: float) -> ratio relativo usando midprice.
    """
    try:
        # Modo snapshot
        if ask is None and isinstance(snapshot_or_bid, dict):
            if not snapshot_or_bid:
                # Tests esperan 0.0 cuando faltan datos por completo.
                return 0.0
            h = float(snapshot_or_bid.get("high", float("nan")))
            l = float(snapshot_or_bid.get("low", float("nan")))
            c = float(snapshot_or_bid.get("close", float("nan")))
            if not (_is_num(h) and _is_num(l) and _is_num(c)) or h < l:
                return None
            spread_abs = h - l
            if c <= 0:
                # Según tests: tratar como 0.0 (no penalizar)
                return 0.0
            return spread_abs / c

        # Modo bid/ask
        if snapshot_or_bid is None or ask is None:
            return None
        b = float(snapshot_or_bid)
        a = float(ask)
        if not (_is_num(b) and _is_num(a)) or a <= 0 or b <= 0 or a < b:
            return None
        mid = (a + b) / 2.0
        if mid <= 0:
            return None
        return (a - b) / mid
    except Exception:
        return None


def spread_gate_default(
    bid: Optional[float],
    ask: Optional[float],
    *,
    max_spread_pct: float = 0.15,  # 0.15% por defecto; ajustable
) -> Tuple[bool, Optional[float]]:
    """
    Acepta/deniega según spread relativo. Retorna (permitido, spread_pct).
    Si no se puede calcular → (False, None).
    """
    r = _approximate_spread(bid, ask)
    if r is None:
        return (False, None)
    try:
        pct = 100.0 * r
        return (pct <= max_spread_pct, pct)
    except Exception:
        return (False, None)


def _resolve_spread_limit(trader: Any, default_ratio: float = 0.0015) -> float:
    """
    Devuelve el límite de spread *relativo* (fracción, no %).
    Si el límite <= 0 → se interpreta como "sin límite" (gate deshabilitado).
    """
    try:
        cfg = getattr(trader, "config", None)
        if cfg is not None and hasattr(cfg, "max_spread_ratio"):
            v = float(getattr(cfg, "max_spread_ratio"))
            return v
    except Exception:
        pass
    try:
        v = float(getattr(trader, "max_spread_ratio", default_ratio))
        return v
    except Exception:
        pass
    return default_ratio


def _spread_gate(
    trader: Any,
    symbol: str,
    snapshot: dict,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Gate solicitado por tests.

    - Devuelve (permitido, ratio, limit) donde ratio/limit son fracciones (no %).
    - Si limit <= 0 → gate deshabilitado → permitido siempre.
    """
    limit = _resolve_spread_limit(trader, default_ratio=0.0015)
    ratio = _approximate_spread(snapshot)
    if ratio is None:
        # Si no se puede calcular, negar pero devolver limit
        return (False, None, limit)
    if limit <= 0:
        # Sin límite: permitir todo
        return (True, ratio, limit)
    return (ratio <= limit, ratio, limit)


@dataclass
class SymbolState:
    """Estado local del pipeline para cada símbolo (solo concerns de procesar_vela)."""
    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=600))
    last_hash: Tuple[int, int] = (0, 0)
    last_df: Optional[pd.DataFrame] = None
    timeframe: Optional[str] = None


class BufferManager:
    """Gestiona buffers por símbolo y genera DataFrames estables con cache por hash."""

    def __init__(self, maxlen: int = 600) -> None:
        self._maxlen = max(100, int(maxlen))
        self._estados: Dict[str, Dict[str, SymbolState]] = {}
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return str(symbol or "").upper()

    @staticmethod
    def _normalize_timeframe(timeframe: Optional[str]) -> Tuple[str, Optional[str]]:
        if timeframe is None:
            return ("default", None)
        tf_label = str(timeframe)
        key = tf_label.lower() or "default"
        return (key, tf_label)

    def _infer_timeframe(self, candle: dict, fallback: Optional[str] = None) -> Optional[str]:
        if not isinstance(candle, dict):
            return fallback
        tf = candle.get("timeframe") or candle.get("interval") or candle.get("tf")
        if tf:
            return str(tf)
        return fallback

    def _get_state(self, symbol: str, timeframe: Optional[str]) -> Tuple[SymbolState, str, Optional[str]]:
        sym = self._normalize_symbol(symbol)
        tf_key, tf_label = self._normalize_timeframe(timeframe)
        estados_symbol = self._estados.setdefault(sym, {})
        st = estados_symbol.get(tf_key)
        if st is None:
            st = SymbolState(deque(maxlen=self._maxlen))
            st.timeframe = tf_label
            estados_symbol[tf_key] = st
        elif tf_label:
            st.timeframe = tf_label
        return st, tf_key, st.timeframe or tf_label

    def get_lock(self, symbol: str, timeframe: Optional[str] = None) -> asyncio.Lock:
        sym = self._normalize_symbol(symbol)
        tf_key, _ = self._normalize_timeframe(timeframe)
        key = (sym, tf_key)
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = asyncio.Lock()
        return lock

    def append(self, symbol: str, candle: dict, timeframe: Optional[str] = None) -> None:
        inferred_tf = self._infer_timeframe(candle, timeframe)
        st, _tf_key, tf_label = self._get_state(symbol, inferred_tf)
        st.buffer.append(candle)
        metric_tf = tf_label or (str(inferred_tf) if inferred_tf else "unknown")
        try:
            safe_set(
                BUFFER_SIZE_V2,
                len(st.buffer),
                timeframe=metric_tf,
            )
        except Exception as exc:
            log.debug("No se pudo actualizar métrica buffer_size_v2: %s", exc)

    def extend(self, symbol: str, candles: Iterable[dict], timeframe: Optional[str] = None) -> None:
        for candle in candles:
            tf = self._infer_timeframe(candle, timeframe)
            self.append(symbol, candle, tf)

    def snapshot(self, symbol: str, timeframe: Optional[str] = None) -> list[dict]:
        st, _tf_key, _tf_label = self._get_state(symbol, timeframe)
        return list(st.buffer)

    def size(self, symbol: str, timeframe: Optional[str] = None) -> int:
        st, _tf_key, _tf_label = self._get_state(symbol, timeframe)
        return len(st.buffer)

    def dataframe(self, symbol: str, timeframe: Optional[str] = None) -> Optional[pd.DataFrame]:
        st, _tf_key, tf_label = self._get_state(symbol, timeframe)
        h = _hash_buffer(st.buffer)
        if h == st.last_hash and st.last_df is not None:
            if tf_label:
                _attach_timeframe(st.last_df, tf_label)
            return st.last_df

        if not st.buffer:
            st.last_df = None
            st.last_hash = h
            return None

        try:
            df = pd.DataFrame(list(st.buffer), columns=COLUMNS)
        except Exception:
            # Saneado por si hay tipos raros en dicts
            try:
                rows = []
                for c in st.buffer:
                    rows.append({
                        "timestamp": int(c.get("timestamp", 0)),
                        "open": float(c.get("open", "nan")),
                        "high": float(c.get("high", "nan")),
                        "low": float(c.get("low", "nan")),
                        "close": float(c.get("close", "nan")),
                        "volume": float(c.get("volume", "nan")),
                    })
                df = pd.DataFrame(rows, columns=COLUMNS)
            except Exception:
                st.last_df = None
                st.last_hash = h
                return None

        # Orden estable por timestamp ascendente y coerción básica
        try:
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["timestamp", "close"]).copy()
            df = df.sort_values("timestamp", kind="mergesort")
        except Exception:
            pass

        _attach_timeframe(df, tf_label)
        st.last_df = df
        st.last_hash = h
        return df


# Instancia de módulo (compartida)
_BUFFER_MAXLEN = max(600, int(os.getenv("PROCESAR_VELA_BUFFER_MAXLEN", "1500")))
_buffers = BufferManager(maxlen=_BUFFER_MAXLEN)


def get_buffer_manager() -> BufferManager:
    """Devuelve el administrador de buffers global utilizado por el pipeline."""

    return _buffers


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ──────────────────────────────────────────────────────────────────────────────

async def procesar_vela(trader: Any, vela: dict) -> None:
    """
    Handler de vela cerrada.
    Requisitos de `trader`:
      - atributos: config, spread_guard (opcional), orders (OrderManager), estado (dict) si quieres estados extra
      - método: evaluar_condiciones_de_entrada(symbol, df, estado)
      - método opcional: enqueue_notification(mensaje, nivel)
    """
    t0 = time.perf_counter()
    symbol = ""
    timeframe_label = "unknown"
    timeframe_hint: Optional[str] = None
    if isinstance(vela, dict):
        raw_symbol = vela.get("symbol") or vela.get("par") or ""
        symbol = str(raw_symbol).upper() if raw_symbol else ""
        config_interval = getattr(getattr(trader, "config", None), "intervalo_velas", None)
        timeframe_hint = (
            vela.get("timeframe")
            or vela.get("interval")
            or vela.get("tf")
            or config_interval
        )
        if timeframe_hint:
            timeframe_label = str(timeframe_hint)
    stage_durations: dict[str, float] = {}
    stage_ends: dict[str, float] = {}
    parse_end: float | None = None
    gating_end: float | None = None
    strategy_end: float | None = None
    bus = getattr(trader, "event_bus", None) or getattr(trader, "bus", None)

    def _mark_stage(stage: str, start_time: float | None) -> float:
        existing = stage_ends.get(stage)
        if existing is not None:
            return existing
        end_time = time.perf_counter()
        base = start_time if start_time is not None else t0
        duration = max(0.0, end_time - base)
        stage_durations[stage] = duration
        stage_ends[stage] = end_time
        return end_time

    def _ensure_gating_end() -> float:
        nonlocal parse_end, gating_end
        if parse_end is None:
            parse_end = _mark_stage("parse", t0)
        if gating_end is None:
            gating_end = _mark_stage("gating", parse_end)
        return gating_end

    def _ensure_strategy_end() -> float:
        nonlocal parse_end, gating_end, strategy_end
        if gating_end is None:
            _ensure_gating_end()
        if strategy_end is None:
            strategy_end = _mark_stage("strategy", gating_end)
        return strategy_end
    
    try:
        if not isinstance(vela, dict):
            parse_end = _mark_stage("parse", t0)
            return
            
        vela.pop("_df_skip_reason", None)
        vela.pop("_df_skip_details", None)
        # 1) Validaciones mínimas y normalización
        if not symbol:
            parse_end = _mark_stage("parse", t0)
            safe_inc(CANDLES_IGNORADAS, reason="no_symbol")
            _mark_skip(vela, "no_symbol")
            return

        ok, reason = _validar_candle(vela)
        if not ok:
            parse_end = _mark_stage("parse", t0)
            safe_inc(CANDLES_IGNORADAS, reason=reason)
            _mark_skip(vela, reason)
            return
        
        if parse_end is None:
            parse_end = _mark_stage("parse", t0)

        # 2) Control por spread (si hay guardia y si el dato viene en la vela)
        spread_ratio = vela.get("spread_ratio") or vela.get("spread")
        sg = getattr(trader, "spread_guard", None)
        if spread_ratio is not None and sg is not None:
            try:
                if hasattr(sg, "allows"):
                    if not sg.allows(symbol, float(spread_ratio)):
                        _ensure_gating_end()
                        safe_inc(
                            ENTRADAS_RECHAZADAS_V2,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        _mark_skip(
                            vela,
                            "spread_guard",
                            {"ratio": float(spread_ratio)},
                        )
                        lock = _buffers.get_lock(symbol, timeframe_hint)
                        async with lock:
                            _buffers.append(symbol, vela, timeframe=timeframe_hint)
                        return
                elif hasattr(sg, "permite_entrada"):
                    if not bool(sg.permite_entrada(symbol, {}, 0.0)):
                        _ensure_gating_end()
                        safe_inc(
                            ENTRADAS_RECHAZADAS_V2,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        _mark_skip(vela, "spread_guard")
                        lock = _buffers.get_lock(symbol, timeframe_hint)
                        async with lock:
                            _buffers.append(symbol, vela, timeframe=timeframe_hint)
                        return
            except Exception:
                # No bloquear por fallo del guard
                pass

        # 3) Append al buffer por símbolo (protegido por lock)
        buffer_timeframe = timeframe_hint
        lock = _buffers.get_lock(symbol, buffer_timeframe)
        async with lock:
            _buffers.append(symbol, vela, timeframe=buffer_timeframe)
            df = _buffers.dataframe(symbol, timeframe=buffer_timeframe)

        if df is None or df.empty:
            _ensure_gating_end()
            safe_inc(CANDLES_IGNORADAS, reason="empty_df")
            _mark_skip(vela, "empty_df")
            return
        
        timeframe = getattr(df, "tf", None)
        if timeframe:
            timeframe = str(timeframe)
            timeframe_label = timeframe
        else:
            if timeframe_hint:
                timeframe = str(timeframe_hint)
                timeframe_label = timeframe

        ready_checker = getattr(trader, "is_symbol_ready", None)
        if callable(ready_checker) and not ready_checker(symbol, timeframe_label):
            _ensure_gating_end()
            safe_inc(CANDLES_IGNORADAS, reason="backfill_pending")
            _mark_skip(vela, "backfill_pending")
            return

        min_needed = _resolve_min_bars(trader)

        try:
            faltan = max(0, min_needed - len(df)) if min_needed > 0 else 0
            safe_set(
                WARMUP_RESTANTE,
                float(faltan),
                symbol=symbol,
                timeframe=(timeframe_label or "unknown"),
            )
        except Exception:
            pass

        try:
            last_ts = _normalize_timestamp(df.iloc[-1]["timestamp"])
            if last_ts is not None:
                edad = max(0.0, time.time() - last_ts)
                safe_set(
                    LAST_BAR_AGE,
                    float(edad),
                    symbol=symbol,
                    timeframe=(timeframe_label or "unknown"),
                )
        except Exception:
            pass

        # 4) Fast-path si estás bajo presión
        cfg = getattr(trader, "config", None)
        fast_enabled = bool(getattr(cfg, "trader_fastpath_enabled", True))
        if fast_enabled:
            threshold = int(getattr(cfg, "trader_fastpath_threshold", 350))
            if len(df) >= threshold and getattr(cfg, "trader_fastpath_skip_entries", True):
                _ensure_gating_end()
                safe_inc(
                    ENTRADAS_RECHAZADAS_V2,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="fastpath_skip_entries",
                )
                _mark_skip(vela, "fastpath_skip_entries", {"buffer_len": len(df), "threshold": threshold})
                return

        # 5) Estado por símbolo (compatible con Trader.estado)
        estado_trader = getattr(trader, "estado", None)
        estado_symbol = estado_trader.get(symbol) if isinstance(estado_trader, dict) else None

        # 6) Evaluar condiciones de entrada (delegado al Trader)
        timeframe = timeframe or timeframe_label
        timeframe_label = str(timeframe or timeframe_label or "unknown")
        log.debug(
            "pre-eval",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "buffer_len": len(df),
                "min_needed": min_needed,
            },
        )
        log.debug(
            "go_evaluate",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "reason": "closed_bar",
            },
        )
        try:
            safe_inc(
                EVAL_INTENTOS_TOTAL,
                symbol=symbol,
                timeframe=timeframe,
                etapa="entrada",
            )
        except Exception:
            pass
        _ensure_gating_end()
        propuesta = await trader.evaluar_condiciones_de_entrada(symbol, df, estado_symbol)
        _ensure_gating_end()
        skip_reason = getattr(trader, "_last_eval_skip_reason", None)
        skip_details = getattr(trader, "_last_eval_skip_details", None)
        score = None
        if isinstance(propuesta, dict):
            score = propuesta.get("score")
        log.debug(
            "entrada_verificada",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "permitida": bool(propuesta),
                "score": score,
            },
        )
        if not propuesta:
            derived_score: Any | None = score
            if derived_score is None and isinstance(skip_details, dict):
                derived_score = skip_details.get("score")
            if skip_reason == "pipeline_missing":
                log.debug(
                    "pipeline_missing_info",
                    extra={"symbol": symbol, "timeframe": timeframe_label},
                )
                _mark_skip(
                    vela,
                    skip_reason,
                    skip_details,
                    gate=skip_reason,
                    score=derived_score,
                )
                return
            if skip_reason:
                _mark_skip(
                    vela,
                    skip_reason,
                    skip_details,
                    gate=skip_reason,
                    score=derived_score,
                )
            else:
                provider = getattr(trader, "_verificar_entrada_provider", None)
                details = {"provider": provider} if provider else None
                _mark_skip(vela, "no_signal", details, score=derived_score)
            return

        side = str(propuesta.get("side", "long")).lower()
        safe_inc(ENTRADAS_CANDIDATAS, symbol=symbol, side=side)

        # 7) Validación final
        precio = float(propuesta.get("precio_entrada", df.iloc[-1]["close"]))
        if not _is_num(precio) or precio <= 0:
            safe_inc(
                ENTRADAS_RECHAZADAS_V2,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="bad_price",
            )
            _mark_skip(vela, "bad_price", score=score)
            return

        # 8) Apertura de orden
        orders = getattr(trader, "orders", None)
        if orders is None or not hasattr(orders, "crear"):
            safe_inc(
                ENTRADAS_RECHAZADAS_V2,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="orders_missing",
            )
            _mark_skip(vela, "orders_missing", score=score)
            return

        sl = float(propuesta.get("stop_loss", 0.0))
        tp = float(propuesta.get("take_profit", 0.0))
        meta = dict(propuesta.get("meta", {}))
        meta.update({"score": propuesta.get("score")})

        try:
            obtener = getattr(orders, "obtener", None)
            ya = obtener(symbol) if callable(obtener) else None
            if ya is not None:
                safe_inc(
                    ENTRADAS_RECHAZADAS_V2,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="ya_abierta",
                )
                _mark_skip(vela, "ya_abierta", score=score)
                return
        except Exception:
            pass

        try:
            await _abrir_orden(orders, symbol, side, precio, sl, tp, meta)
            safe_inc(ENTRADAS_ABIERTAS, symbol=symbol, side=side)
            notify = getattr(trader, "enqueue_notification", None)
            if callable(notify):
                notify(f"Abrir {side} {symbol} @ {precio:.6f}", "INFO")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            safe_inc(HANDLER_EXCEPTIONS)
            log.exception("Error abriendo orden para %s: %s", symbol, e)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        safe_inc(HANDLER_EXCEPTIONS)
        log.exception("Excepción en procesar_vela: %s", e)
    finally:
        try:
            if symbol:
                safe_inc(
                    CANDLES_PROCESSED_TOTAL,
                    symbol=symbol,
                    timeframe=(timeframe_label or "unknown"),
                )
        except Exception:
            pass
        try:
            stage_metric_map = {
                "parse": PARSE_LATENCY,
                "gating": GATING_LATENCY,
                "strategy": STRATEGY_LATENCY,
            }
            emit_fn = getattr(bus, "emit", None)
            for stage in ("parse", "gating", "strategy"):
                duration = stage_durations.get(stage)
                if duration is None:
                    continue
                metric = stage_metric_map.get(stage)
                if metric is not None:
                    metric.observe(duration)
                if callable(emit_fn):
                    payload = {
                        "stage": stage,
                        "duration": duration,
                        "symbol": symbol,
                        "timeframe": timeframe_label,
                    }
                    try:
                        emit_fn("procesar_vela.latency", payload)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            EVAL_LATENCY.observe(time.perf_counter() - t0)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de apertura de orden
# ──────────────────────────────────────────────────────────────────────────────

async def _abrir_orden(
    orders: Any,
    symbol: str,
    side: str,
    precio: float,
    sl: float,
    tp: float,
    meta: Dict[str, Any],
) -> None:
    """Wrapper robusto sobre OrderManager.crear(...)."""
    crear = getattr(orders, "crear", None)
    if not callable(crear):
        raise RuntimeError("OrderManager no implementa crear(...)")

    if precio <= 0:
        raise ValueError("precio_entrada inválido")
    if sl <= 0 or tp <= 0:
        log.debug("[%s] SL/TP no establecidos al abrir (sl=%.6f, tp=%.6f)", symbol, sl, tp)

    res = crear(symbol=symbol, side=side, precio=precio, sl=sl, tp=tp, meta=meta)
    if asyncio.iscoroutine(res):
        await res






