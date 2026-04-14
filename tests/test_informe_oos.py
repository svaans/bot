"""Informe OOS y walk-forward (diag)."""

from __future__ import annotations

import pandas as pd

from core.diag.informe_oos import (
    informe_split_unico,
    informe_walk_forward,
    walk_forward_splits,
)


def test_walk_forward_splits_tamanos() -> None:
    df = pd.DataFrame({"x": range(20)})
    pairs = walk_forward_splits(df, n_folds=4, min_train_rows=5)
    assert len(pairs) == 3
    assert len(pairs[0][0]) == 5 and len(pairs[0][1]) == 5
    assert len(pairs[2][0]) == 15 and len(pairs[2][1]) == 5


def test_informe_split_unico_train_vs_test() -> None:
    rows = []
    for i in range(8):
        rows.append(
            {
                "timestamp": float(i),
                "retorno_total": 10.0,
                "estrategias_activas": {"a": True},
            }
        )
    for i in range(8, 10):
        rows.append(
            {
                "timestamp": float(i),
                "retorno_total": -5.0,
                "estrategias_activas": {"a": True},
            }
        )
    df = pd.DataFrame(rows)
    rep = informe_split_unico(df, test_ratio=0.2)
    row = rep.loc[rep["estrategia"] == "a"].iloc[0]
    assert int(row["n_train"]) == 8
    assert int(row["n_test"]) == 2
    assert float(row["retorno_medio_train"]) > float(row["retorno_medio_test"])


def test_informe_walk_forward_agrega() -> None:
    rows = []
    for i in range(24):
        r = 1.0 if i < 18 else -2.0
        rows.append(
            {
                "timestamp": float(i),
                "retorno_total": r,
                "estrategias_activas": {"b": True},
            }
        )
    df = pd.DataFrame(rows)
    wf = informe_walk_forward(df, n_folds=4)
    assert not wf.empty
    b = wf.loc[wf["estrategia"] == "b"].iloc[0]
    assert int(b["pliegues_con_presencia"]) >= 1
