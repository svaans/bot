"""Log append-only de ejecuciones (slippage / fills)."""
from __future__ import annotations

from pathlib import Path

import pytest


def test_append_and_load_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "ej.jsonl"
    monkeypatch.setenv("EXECUTION_QUALITY_LOG_PATH", str(p))

    from core.diag import execution_quality_log as eq

    eq.append_ejecucion_mercado({"symbol": "X", "slippage_vs_ticker": 0.01})
    rows = eq.load_ejecuciones_jsonl()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "X"


def test_append_disabled_does_not_create_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "ej.jsonl"
    monkeypatch.setenv("EXECUTION_QUALITY_LOG_PATH", str(p))
    monkeypatch.setenv("EXECUTION_QUALITY_LOG_ENABLED", "0")

    from core.diag import execution_quality_log as eq

    eq.append_ejecucion_mercado({"x": 1})
    assert not p.exists()


def test_informe_resumen_por_simbolo() -> None:
    from core.diag.informe_ejecucion import _resumen_por_simbolo
    import pandas as pd

    df = pd.DataFrame(
        [
            {"symbol": "A/B", "slippage_vs_ticker": 0.1, "slippage_vs_senal": 0.2},
            {"symbol": "A/B", "slippage_vs_ticker": 0.3, "slippage_vs_senal": 0.4},
        ]
    )
    r = _resumen_por_simbolo(df)
    assert len(r) == 1
    assert r.iloc[0]["symbol"] == "A/B"
    assert r.iloc[0]["n"] == 2
