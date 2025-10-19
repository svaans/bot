from __future__ import annotations

import pandas as pd

from core.procesar_vela import BufferManager
from indicadores.incremental import (
    actualizar_momentum_incremental,
    actualizar_rsi_incremental,
)


def _build_df(closes: list[float], *, start_ts: int = 1_700_000_000_000) -> pd.DataFrame:
    rows: list[dict[str, float | int | bool]] = []
    for idx, close in enumerate(closes):
        ts = start_ts + idx * 60_000
        rows.append(
            {
                "timestamp": ts,
                "open": float(close) - 0.5,
                "high": float(close) + 1.0,
                "low": float(close) - 1.0,
                "close": float(close),
                "volume": 1_000.0 + idx,
                "is_closed": True,
            }
        )
    return pd.DataFrame(rows)


def test_incremental_cache_tracks_progress_and_persists_on_appends() -> None:
    estado: dict[str, object] = {}
    df = _build_df([float(100 + i) for i in range(20)])

    actualizar_rsi_incremental(estado, df)
    cache = estado.get("indicadores_cache")
    assert isinstance(cache, dict)
    meta = cache.get("_meta")
    assert isinstance(meta, dict)
    expected_ts = (1_700_000_000_000 + 19 * 60_000) / 1000.0
    assert meta.get("last_timestamp") == expected_ts
    assert meta.get("last_close") == float(119)

    cache["sentinel"] = "keep"

    df_append = _build_df([float(100 + i) for i in range(21)])
    actualizar_rsi_incremental(estado, df_append)

    cache_after = estado.get("indicadores_cache")
    assert isinstance(cache_after, dict)
    assert cache_after.get("sentinel") == "keep"
    meta_after = cache_after.get("_meta")
    assert isinstance(meta_after, dict)
    expected_ts_append = (1_700_000_000_000 + 20 * 60_000) / 1000.0
    assert meta_after.get("last_timestamp") == expected_ts_append
    assert meta_after.get("last_close") == float(120)


def test_incremental_cache_resets_on_timestamp_rewind() -> None:
    estado: dict[str, object] = {}
    df = _build_df([float(100 + i) for i in range(20)])
    actualizar_rsi_incremental(estado, df)

    cache = estado.get("indicadores_cache")
    assert isinstance(cache, dict)
    cache["sentinel"] = "keep"

    rewind_df = _build_df([float(100 + i) for i in range(15)])
    actualizar_rsi_incremental(estado, rewind_df)

    cache_after = estado.get("indicadores_cache")
    assert isinstance(cache_after, dict)
    assert "sentinel" not in cache_after
    meta = cache_after.get("_meta")
    assert isinstance(meta, dict)
    expected_ts = (1_700_000_000_000 + 14 * 60_000) / 1000.0
    assert meta.get("last_timestamp") == expected_ts
    assert meta.get("last_close") == float(114)


def test_incremental_cache_resets_on_close_correction() -> None:
    estado: dict[str, object] = {}
    df = _build_df([float(100 + i) for i in range(20)])
    actualizar_momentum_incremental(estado, df)

    cache = estado.get("indicadores_cache")
    assert isinstance(cache, dict)
    cache["sentinel"] = "keep"

    correction_df = df.copy(deep=True)
    correction_df.iloc[-1, correction_df.columns.get_loc("close")] = 999.0
    correction_df.iloc[-1, correction_df.columns.get_loc("high")] = 1_000.0
    correction_df.iloc[-1, correction_df.columns.get_loc("low")] = 998.0

    actualizar_momentum_incremental(estado, correction_df)

    cache_after = estado.get("indicadores_cache")
    assert isinstance(cache_after, dict)
    assert "sentinel" not in cache_after
    meta = cache_after.get("_meta")
    assert isinstance(meta, dict)
    expected_ts = (1_700_000_000_000 + 19 * 60_000) / 1000.0
    assert meta.get("last_timestamp") == expected_ts
    assert meta.get("last_close") == float(999.0)


def test_buffer_manager_clear_invalidates_incremental_context() -> None:
    manager = BufferManager(maxlen=5)
    candle = {
        "timestamp": 1_700_000_000_000,
        "open": 100.0,
        "high": 101.0,
        "low": 99.5,
        "close": 100.5,
        "volume": 1_000.0,
        "is_closed": True,
    }

    manager.append("BTCUSDT", candle)
    state = manager.state("BTCUSDT")
    cache_ref = state.indicators_state
    cache_ref["foo"] = "bar"
    cache_ref["_meta"] = {"last_timestamp": 1.0, "last_close": 100.5}

    manager.clear("BTCUSDT", drop_state=True)

    meta = cache_ref.get("_meta")
    assert isinstance(meta, dict)
    assert meta.get("invalidated") is True
    assert "foo" not in cache_ref


def test_buffer_manager_get_indicator_value_returns_expected_payload() -> None:
    manager = BufferManager(maxlen=5)
    state = manager.state("BTCUSDT", "1h")
    payload = {"valor": 55.5, "periodo": 14}
    state.indicators_state["rsi"] = payload

    assert manager.get_indicator_value("BTCUSDT", "1h", "rsi") == 55.5
    assert manager.get_indicator_value("BTCUSDT", "1h", "RSI") == 55.5
    assert manager.get_indicator_value("BTCUSDT", "1h", "rsi", value_key=None) is payload
    assert (
        manager.get_indicator_value("BTCUSDT", "1h", "momentum", default=None)
        is None
    )