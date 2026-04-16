# core/vela/buffers.py — buffers por símbolo/timeframe del pipeline
from __future__ import annotations

import asyncio
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, Optional, Tuple

import pandas as pd

from core.metrics_helpers import safe_set
from core.utils.logger import configurar_logger

from core.vela.helpers import (
    COLUMNS,
    _attach_symbol,
    _attach_timeframe,
    _dataframe_fingerprint,
    _hash_buffer,
)

log = configurar_logger("procesar_vela")


def _buffer_size_metric() -> Any:
    """Resuelve la métrica vía :mod:`core.vela.pipeline` para que los tests puedan parchearla."""
    from core.vela import pipeline as vpl  # import diferido: evita ciclo y respeta monkeypatch

    return vpl.BUFFER_SIZE_V2


@dataclass
class SymbolState:
    """Estado local del pipeline para cada símbolo (solo concerns de procesar_vela)."""
    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=600))
    last_hash: Tuple[int, int] = (0, 0)
    last_df: Optional[pd.DataFrame] = None
    timeframe: Optional[str] = None
    indicators_state: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Alias para compatibilidad con consumidores previos que esperan
        # ``indicadores_cache`` en el estado incremental.
        if not hasattr(self, "indicadores_cache"):
            object.__setattr__(self, "indicadores_cache", self.indicators_state)


class BufferManager:
    """Gestiona buffers por símbolo y genera DataFrames estables con cache por hash.

    El estado es compartido entre todas las estrategias y datafeeds del proceso.
    Para tareas de backfill manual donde se reemplaza el origen de datos
    (histórico vs. en vivo) es necesario limpiar los buffers de un símbolo o
    timeframe específico para evitar mezclar velas incompatibles. Utiliza
    :meth:`clear` para sincronizar el estado antes de reanudar el flujo en vivo.
    """

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
                _buffer_size_metric(),
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

    def clear(
        self,
        symbol: str,
        timeframe: Optional[str] = None,
        *,
        drop_state: bool = False,
    ) -> None:
        """Limpia el buffer de un símbolo/timeframe.

        Args:
            symbol: Par de trading cuyos datos deben limpiarse.
            timeframe: Intervalo a limpiar. ``None`` elimina todos los intervalos
                asociados al símbolo.
            drop_state: Cuando es ``True`` se elimina el ``SymbolState`` completo
                (incluyendo locks asociados) en lugar de solo vaciar el buffer.

        Se expone como API pública para permitir que procesos de backfill manual
        alineen los buffers internos con la fuente histórica antes de reanudar el
        stream en vivo, evitando inconsistencias entre datasets.
        """

        sym = self._normalize_symbol(symbol)
        tf_key, _ = self._normalize_timeframe(timeframe)
        estados_symbol = self._estados.get(sym)
        if not estados_symbol:
            return

        items: Iterable[tuple[str, SymbolState]]
        if timeframe is None:
            items = list(estados_symbol.items())
        else:
            st = estados_symbol.get(tf_key)
            if st is None:
                return
            items = [(tf_key, st)]

        for key, st in items:
            st.buffer.clear()
            st.last_df = None
            st.last_hash = (0, 0)
            st.indicators_state.clear()
            if drop_state:
                meta = st.indicators_state.get("_meta")
                if not isinstance(meta, dict):
                    meta = {}
                meta.clear()
                meta["invalidated"] = True
                st.indicators_state["_meta"] = meta
            metric_tf = st.timeframe or (key if key != "default" else "unknown")
            try:
                safe_set(_buffer_size_metric(), 0, timeframe=metric_tf)
            except Exception as exc:  # pragma: no cover - métricas opcionales
                log.debug("No se pudo reiniciar métrica buffer_size_v2: %s", exc)

            if drop_state:
                estados_symbol.pop(key, None)
                self._locks.pop((sym, key), None)

        if drop_state and not estados_symbol:
            self._estados.pop(sym, None)

    def state(self, symbol: str, timeframe: Optional[str] = None) -> SymbolState:
        """Devuelve el ``SymbolState`` asociado al símbolo/timeframe indicado."""

        st, _tf_key, _tf_label = self._get_state(symbol, timeframe)
        return st

    def get_indicator_value(
        self,
        symbol: str,
        timeframe: Optional[str],
        indicator: str,
        *,
        value_key: str = "valor",
        default: Any = None,
    ) -> Any:
        """Obtiene un valor incremental almacenado en el ``SymbolState``.

        Args:
            symbol: Par de trading cuyo estado se desea consultar.
            timeframe: Intervalo asociado al cálculo incremental. Utiliza
                ``None`` para el timeframe por defecto.
            indicator: Nombre del indicador almacenado en ``indicators_state``.
            value_key: Clave a devolver del diccionario del indicador. Por
                defecto ``"valor"`` para mantener compatibilidad con los
                incrementales actuales.
            default: Valor a devolver cuando no exista información
                almacenada.

        Returns:
            El valor solicitado o ``default`` si no se encuentra.
        """

        if not indicator:
            return default

        state = self.state(symbol, timeframe)
        store = state.indicators_state

        indicator_key = str(indicator)
        entry = store.get(indicator_key)
        if entry is None and indicator_key.lower() != indicator_key:
            entry = store.get(indicator_key.lower())
        if entry is None and indicator_key.upper() != indicator_key:
            entry = store.get(indicator_key.upper())

        if entry is None:
            return default

        if value_key is None:
            return entry

        if isinstance(entry, dict) and value_key in entry:
            return entry[value_key]

        return default

    def dataframe(self, symbol: str, timeframe: Optional[str] = None) -> Optional[pd.DataFrame]:
        st, _tf_key, tf_label = self._get_state(symbol, timeframe)
        h = _hash_buffer(st.buffer)
        normalized_symbol = self._normalize_symbol(symbol)
        prev_df = st.last_df if isinstance(st.last_df, pd.DataFrame) else None
        prev_fp = _dataframe_fingerprint(prev_df)

        if h == st.last_hash and st.last_df is not None:
            if tf_label:
                _attach_timeframe(st.last_df, tf_label)
            _attach_symbol(st.last_df, normalized_symbol)
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

        fingerprint = _dataframe_fingerprint(df)
        df.attrs["_indicators_cache_fingerprint"] = fingerprint

        if prev_df is not None:
            prev_cache = prev_df.attrs.get("_indicators_cache")
            appended_only = (
                prev_fp[0] > 0
                and fingerprint[0] == prev_fp[0] + 1
                and fingerprint[1] >= prev_fp[1]
            )
            if appended_only and hasattr(prev_cache, "get_or_compute") and hasattr(prev_cache, "set"):
                df.attrs["_indicators_cache"] = prev_cache

        _attach_timeframe(df, tf_label)
        st.last_df = df
        _attach_symbol(df, normalized_symbol)
        st.last_hash = h
        return df


_BUFFER_MAXLEN = max(600, int(os.getenv("PROCESAR_VELA_BUFFER_MAXLEN", "1500")))


def create_buffer_manager(maxlen: int | None = None) -> BufferManager:
    """Crea un ``BufferManager`` (p. ej. uno por ``Trader`` si ``trader_buffer_isolated``)."""

    ml = maxlen if maxlen is not None else _BUFFER_MAXLEN
    return BufferManager(maxlen=ml)


_buffers = create_buffer_manager(_BUFFER_MAXLEN)


def get_buffer_manager() -> BufferManager:
    """Devuelve el administrador de buffers global utilizado por el pipeline."""

    return _buffers


def _resolve_buffer_manager(trader: Any) -> BufferManager:
    bm = getattr(trader, "buffer_manager", None)
    if isinstance(bm, BufferManager):
        return bm
    return _buffers
