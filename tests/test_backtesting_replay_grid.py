"""Replay y rejilla sobre CSV mínimo (sin red)."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from backtesting.grid_search import merge_config, run_grid_search_async, sort_results_by_permitido_rate
from backtesting.replay import load_ohlcv_csv, replay_entradas_async, summarize_replay


def _synth_ohlcv(n: int = 80) -> pd.DataFrame:
    base = 100.0
    rows = []
    ts = 1_700_000_000_000
    for i in range(n):
        o = base + 0.1 * i
        c = o + 0.05
        rows.append(
            {
                "timestamp": ts + i * 300_000,
                "open": o,
                "high": c + 0.2,
                "low": o - 0.2,
                "close": c,
                "volume": 50.0 + i,
            }
        )
    return pd.DataFrame(rows)


@pytest.mark.asyncio
async def test_replay_and_summarize(tmp_path) -> None:
    csv_p = tmp_path / "x.csv"
    _synth_ohlcv(80).to_csv(csv_p, index=False)

    df = load_ohlcv_csv(csv_p)
    rows = await replay_entradas_async(
        "BTC/EUR",
        df,
        window=40,
        step=10,
        config={"umbral_score_tecnico": 0.01, "diversidad_minima": 1, "usar_score_tecnico": False},
    )
    assert len(rows) >= 4
    s = summarize_replay(rows)
    assert s["evaluaciones"] == len(rows)
    assert 0.0 <= s["permitido_rate"] <= 1.0


@pytest.mark.asyncio
async def test_grid_relaxes_umbral_changes_permitido_rate(tmp_path) -> None:
    csv_p = tmp_path / "y.csv"
    _synth_ohlcv(90).to_csv(csv_p, index=False)

    grid = {"umbral_score_tecnico": [5.0, 0.01], "diversidad_minima": [1], "usar_score_tecnico": [False]}
    results = await run_grid_search_async(
        csv_p,
        "BTC/EUR",
        window=45,
        step=15,
        base_config=None,
        grid=grid,
    )
    assert len(results) == 2
    by_rate = sort_results_by_permitido_rate(results)
    strict = next(r for r in results if float(r["overrides"]["umbral_score_tecnico"]) == 5.0)
    loose = next(r for r in results if float(r["overrides"]["umbral_score_tecnico"]) == 0.01)
    assert loose["summary"]["permitido_rate"] >= strict["summary"]["permitido_rate"]
    assert by_rate[0]["summary"]["permitido_rate"] == max(r["summary"]["permitido_rate"] for r in results)


def test_merge_config() -> None:
    assert merge_config({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}
    assert merge_config({"a": 1}, {"a": 3}) == {"a": 3}


def test_load_grid_json_roundtrip(tmp_path) -> None:
    from backtesting.grid_search import load_grid_json

    p = tmp_path / "g.json"
    p.write_text(json.dumps({"umbral_score_tecnico": [1.0, 2.0], "diversidad_minima": [2]}), encoding="utf-8")
    g = load_grid_json(p)
    assert g["umbral_score_tecnico"] == [1.0, 2.0]
