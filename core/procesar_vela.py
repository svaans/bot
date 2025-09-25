# core/procesar_vela.py
from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional, Tuple

import pandas as pd

from core.utils.logger import configurar_logger
from core.utils.metrics_compat import Counter, Gauge, Histogram

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
BUFFERS_TAM = Gauge(
    "procesar_vela_buffer_size",
    "Tamaño del buffer de velas por símbolo",
    ["symbol"],
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
ENTRADAS_RECHAZADAS = Counter(
    "procesar_vela_entradas_rechazadas_total",
    "Entradas rechazadas tras validaciones finales",
    ["symbol", "reason"],
)

log = configurar_logger("procesar_vela")

# ──────────────────────────────────────────────────────────────────────────────
# Utilidades y estructuras internas
# ──────────────────────────────────────────────────────────────────────────────

COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")

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


# ──────────────────────────────────────────────────────────────────────────────
# Helpers solicitados por tests: spread
# ──────────────────────────────────────────────────────────────────────────────

def _approximate_spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """
    Devuelve spread (ask - bid) si ambos existen y son coherentes.
    None si falta dato o ask < bid.
    """
    try:
        if bid is None or ask is None:
            return None
        b = float(bid)
        a = float(ask)
        s = a - b
        if s < 0:
            return None
        return s
    except Exception:
        return None


def spread_gate_default(
    bid: Optional[float],
    ask: Optional[float],
    *,
    max_spread_pct: float = 0.15,  # 0.15% por defecto; ajustable en tests
) -> Tuple[bool, Optional[float]]:
    """
    Acepta/deniega según spread relativo. Retorna (permitido, spread_pct).
    Si no se puede calcular → (False, None).
    """
    s = _approximate_spread(bid, ask)
    if s is None:
        return (False, None)
    try:
        mid = (float(bid) + float(ask)) / 2.0
        if mid <= 0:
            return (False, None)
        pct = 100.0 * (s / mid)
        return (pct <= max_spread_pct, pct)
    except Exception:
        return (False, None)


def _spread_gate(
    bid: Optional[float],
    ask: Optional[float],
    *,
    max_spread_pct: float = 0.15,
) -> bool:
    """
    Gate simplificado solicitado por tests: devuelve solo el booleano (permitido).
    Implementado como envoltorio de `spread_gate_default`.
    """
    ok, _pct = spread_gate_default(bid, ask, max_spread_pct=max_spread_pct)
    return ok


@dataclass
class SymbolState:
    """Estado local del pipeline para cada símbolo (solo concerns de procesar_vela)."""
    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=600))
    last_hash: Tuple[int, int] = (0, 0)
    last_df: Optional[pd.DataFrame] = None


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
        st = self._get_state(symbol)
        st.buffer.append(candle)
        BUFFERS_TAM.labels(symbol=symbol).set(len(st.buffer))

    def dataframe(self, symbol: str) -> Optional[pd.DataFrame]:
        st = self._get_state(symbol)
        h = _hash_buffer(st.buffer)
        if h == st.last_hash and st.last_df is not None:
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

        st.last_df = df
        st.last_hash = h
        return df


# Instancia de módulo (compartida)
_buffers = BufferManager(maxlen=600)


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
                        ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="spread_guard").inc()
                        lock = _buffers.get_lock(symbol)
                        async with lock:
                            _buffers.append(symbol, vela)
                        return
                elif hasattr(sg, "permite_entrada"):
                    if not bool(sg.permite_entrada(symbol, {}, 0.0)):
                        ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="spread_guard").inc()
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

        # 4) Fast-path si estás bajo presión
        cfg = getattr(trader, "config", None)
        fast_enabled = bool(getattr(cfg, "trader_fastpath_enabled", True))
        if fast_enabled:
            threshold = int(getattr(cfg, "trader_fastpath_threshold", 350))
            if len(df) >= threshold and getattr(cfg, "trader_fastpath_skip_entries", True):
                ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="fastpath_skip_entries").inc()
                return

        # 5) Estado por símbolo (compatible con Trader.estado)
        estado_trader = getattr(trader, "estado", None)
        estado_symbol = estado_trader.get(symbol) if isinstance(estado_trader, dict) else None

        # 6) Evaluar condiciones de entrada (delegado al Trader)
        propuesta = await trader.evaluar_condiciones_de_entrada(symbol, df, estado_symbol)
        if not propuesta:
            return

        side = str(propuesta.get("side", "long")).lower()
        ENTRADAS_CANDIDATAS.labels(symbol=symbol, side=side).inc()

        # 7) Validación final
        precio = float(propuesta.get("precio_entrada", df.iloc[-1]["close"]))
        if not _is_num(precio) or precio <= 0:
            ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="bad_price").inc()
            return

        # 8) Apertura de orden
        orders = getattr(trader, "orders", None)
        if orders is None or not hasattr(orders, "crear"):
            ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="orders_missing").inc()
            return

        sl = float(propuesta.get("stop_loss", 0.0))
        tp = float(propuesta.get("take_profit", 0.0))
        meta = dict(propuesta.get("meta", {}))
        meta.update({"score": propuesta.get("score")})

        try:
            obtener = getattr(orders, "obtener", None)
            ya = obtener(symbol) if callable(obtener) else None
            if ya is not None:
                ENTRADAS_RECHAZADAS.labels(symbol=symbol, reason="ya_abierta").inc()
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



