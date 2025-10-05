# core/procesar_vela.py
from __future__ import annotations

import asyncio
import contextlib
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
        BUFFER_SIZE,
        BUFFERS_TAM,
        ENTRADAS_RECHAZADAS,
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
    BUFFER_SIZE = _NullMetric()  # type: ignore[assignment]
    BUFFERS_TAM = Gauge(
        "buffers_tam",
        "Tamaño del buffer actual por símbolo y timeframe",
        ["symbol", "timeframe"],
    )
    ENTRADAS_RECHAZADAS = Counter(
        "procesar_vela_entradas_rechazadas_total",
        "Entradas rechazadas tras validaciones finales",
        ["symbol", "timeframe", "reason"],
    )

try:  # Preferir constante compartida para coherencia con warmup
    from core.data.bootstrap import MIN_BARS as _DEFAULT_MIN_BARS
except Exception:  # pragma: no cover - fallback cuando no existe el módulo
    _DEFAULT_MIN_BARS = int(os.getenv("MIN_BARS", "400"))

DEFAULT_MIN_BARS = _DEFAULT_MIN_BARS

# ──────────────────────────────────────────────────────────────────────────────
# Métricas (compatibles aunque no haya prometheus_client)
# ──────────────────────────────────────────────────────────────────────────────
EVAL_LATENCY = Histogram(
    "procesar_vela_eval_latency_seconds",
    "Latencia total de procesar_vela (parseo + buffers + estrategia)",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
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
        self._estados: Dict[str, SymbolState] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_state(self, symbol: str) -> SymbolState:
        sym = symbol.upper()
        st = self._estados.get(sym)
        if st is None:
            st = self._estados[sym] = SymbolState(deque(maxlen=self._maxlen))
        return st

    def get_lock(self, symbol: str) -> asyncio.Lock:
        sym = symbol.upper()
        lock = self._locks.get(sym)
        if lock is None:
            lock = self._locks[sym] = asyncio.Lock()
        return lock

    def append(self, symbol: str, candle: dict) -> None:
        sym = symbol.upper()
        st = self._get_state(sym)
        st.buffer.append(candle)
        tf = candle.get("timeframe") or candle.get("interval") or candle.get("tf")
        if tf:
            st.timeframe = str(tf)
        tf_label = st.timeframe or (str(tf) if tf else None)
        try:
            safe_set(
                BUFFERS_TAM,
                len(st.buffer),
                symbol=sym,
                timeframe=tf_label or "unknown",
            )
        except Exception as exc:
            log.warning("No se pudo actualizar métrica BUFFERS_TAM: %s", exc)
        try:
            if tf_label:
                safe_set(
                    BUFFER_SIZE,
                    len(st.buffer),
                    symbol=sym,
                    timeframe=tf_label,
                )
        except Exception as exc:
            log.debug("No se pudo actualizar métrica buffer_size: %s", exc)

    def extend(self, symbol: str, candles: Iterable[dict]) -> None:
        for candle in candles:
            self.append(symbol, candle)

    def snapshot(self, symbol: str) -> list[dict]:
        st = self._get_state(symbol)
        return list(st.buffer)

    def size(self, symbol: str) -> int:
        st = self._get_state(symbol)
        return len(st.buffer)

    def dataframe(self, symbol: str) -> Optional[pd.DataFrame]:
        st = self._get_state(symbol)
        h = _hash_buffer(st.buffer)
        if h == st.last_hash and st.last_df is not None:
            if st.timeframe:
                _attach_timeframe(st.last_df, st.timeframe)
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

        _attach_timeframe(df, st.timeframe)
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
    try:
        # 1) Validaciones mínimas y normalización
        symbol = str(vela.get("symbol") or vela.get("par") or "").upper()
        if not symbol:
            CANDLES_IGNORADAS.labels(reason="no_symbol").inc()
            return
        
        timeframe_hint = (
            vela.get("timeframe")
            or vela.get("interval")
            or vela.get("tf")
            or getattr(getattr(trader, "config", None), "intervalo_velas", None)
        )
        timeframe_label = str(timeframe_hint) if timeframe_hint else "unknown"

        ok, reason = _validar_candle(vela)
        if not ok:
            CANDLES_IGNORADAS.labels(reason=reason).inc()
            return

        # 2) Control por spread (si hay guardia y si el dato viene en la vela)
        spread_ratio = vela.get("spread_ratio") or vela.get("spread")
        sg = getattr(trader, "spread_guard", None)
        if spread_ratio is not None and sg is not None:
            try:
                if hasattr(sg, "allows"):
                    if not sg.allows(symbol, float(spread_ratio)):
                        safe_inc(
                            ENTRADAS_RECHAZADAS,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        lock = _buffers.get_lock(symbol)
                        async with lock:
                            _buffers.append(symbol, vela)
                        return
                elif hasattr(sg, "permite_entrada"):
                    if not bool(sg.permite_entrada(symbol, {}, 0.0)):
                        safe_inc(
                            ENTRADAS_RECHAZADAS,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        lock = _buffers.get_lock(symbol)
                        async with lock:
                            _buffers.append(symbol, vela)
                        return
            except Exception:
                # No bloquear por fallo del guard
                pass

        # 3) Append al buffer por símbolo (protegido por lock)
        lock = _buffers.get_lock(symbol)
        async with lock:
            _buffers.append(symbol, vela)
            df = _buffers.dataframe(symbol)

        if df is None or df.empty:
            CANDLES_IGNORADAS.labels(reason="empty_df").inc()
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
            CANDLES_IGNORADAS.labels(reason="backfill_pending").inc()
            return

        min_needed = _resolve_min_bars(trader)

        try:
            faltan = max(0, min_needed - len(df)) if min_needed > 0 else 0
            WARMUP_RESTANTE.labels(symbol, timeframe_label).set(faltan)
        except Exception:
            pass

        try:
            last_ts = _normalize_timestamp(df.iloc[-1]["timestamp"])
            if last_ts is not None:
                edad = max(0.0, time.time() - last_ts)
                LAST_BAR_AGE.labels(symbol, timeframe_label).set(edad)
        except Exception:
            pass

        # 4) Fast-path si estás bajo presión
        cfg = getattr(trader, "config", None)
        fast_enabled = bool(getattr(cfg, "trader_fastpath_enabled", True))
        if fast_enabled:
            threshold = int(getattr(cfg, "trader_fastpath_threshold", 350))
            if len(df) >= threshold and getattr(cfg, "trader_fastpath_skip_entries", True):
                safe_inc(
                    ENTRADAS_RECHAZADAS,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="fastpath_skip_entries",
                )
                return

        # 5) Estado por símbolo (compatible con Trader.estado)
        estado_trader = getattr(trader, "estado", None)
        estado_symbol = estado_trader.get(symbol) if isinstance(estado_trader, dict) else None

        # 6) Evaluar condiciones de entrada (delegado al Trader)
        log.debug(
            "pre-eval",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "buffer_len": len(df),
                "min_needed": min_needed,
            },
        )
        propuesta = await trader.evaluar_condiciones_de_entrada(symbol, df, estado_symbol)
        if not propuesta:
            return

        side = str(propuesta.get("side", "long")).lower()
        ENTRADAS_CANDIDATAS.labels(symbol=symbol, side=side).inc()

        # 7) Validación final
        precio = float(propuesta.get("precio_entrada", df.iloc[-1]["close"]))
        if not _is_num(precio) or precio <= 0:
            safe_inc(
                ENTRADAS_RECHAZADAS,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="bad_price",
            )
            return

        # 8) Apertura de orden
        orders = getattr(trader, "orders", None)
        if orders is None or not hasattr(orders, "crear"):
            safe_inc(
                ENTRADAS_RECHAZADAS,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="orders_missing",
            )
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
                    ENTRADAS_RECHAZADAS,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="ya_abierta",
                )
                return
        except Exception:
            pass

        try:
            await _abrir_orden(orders, symbol, side, precio, sl, tp, meta)
            ENTRADAS_ABIERTAS.labels(symbol=symbol, side=side).inc()
            notify = getattr(trader, "enqueue_notification", None)
            if callable(notify):
                notify(f"Abrir {side} {symbol} @ {precio:.6f}", "INFO")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            HANDLER_EXCEPTIONS.inc()
            log.exception("Error abriendo orden para %s: %s", symbol, e)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        HANDLER_EXCEPTIONS.inc()
        log.exception("Excepción en procesar_vela: %s", e)
    finally:
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






