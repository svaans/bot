"""Consolidación de atribución por estrategia."""

from __future__ import annotations

import pandas as pd
import pytest

from core.diag.atribucion_estrategias import atribucion_desde_carpeta


def test_atribucion_desde_carpeta_consolida_simbolos(tmp_path) -> None:
    pytest.importorskip("pyarrow")
    carpeta = tmp_path / "ordenes"
    carpeta.mkdir()
    df1 = pd.DataFrame(
        [
            {
                "timestamp": 1_700_000_000,
                "retorno_total": 100.0,
                "estrategias_activas": {"a": True, "b": True},
                "resultado": "ganancia",
            },
        ]
    )
    df2 = pd.DataFrame(
        [
            {
                "timestamp": 1_700_000_100,
                "retorno_total": -40.0,
                "estrategias_activas": {"a": True},
                "resultado": "perdida",
            },
        ]
    )
    df1.to_parquet(carpeta / "BTC_USDT.parquet", index=False)
    df2.to_parquet(carpeta / "ETH_USDT.parquet", index=False)

    global_df, detalle_df = atribucion_desde_carpeta(carpeta)

    assert not global_df.empty
    row_a = global_df.loc[global_df["estrategia"] == "a"].iloc[0]
    assert int(row_a["total"]) == 2
    assert abs(float(row_a["retorno_total"]) - 10.0) < 1e-6

    assert "symbol" in detalle_df.columns
    assert set(detalle_df["symbol"].unique()) == {"BTC/USDT", "ETH/USDT"}
