"""Pruebas unitarias para la estrategia de inversión del histograma MACD."""

from __future__ import annotations

import pandas as pd
import pytest

from core.strategies.entry.estrategia_macd_hist_inversion import (
    estrategia_macd_hist_inversion,
)


def _build_dataframe(rows: int = 40) -> pd.DataFrame:
    datos = {
        "timestamp": list(range(rows)),
        "open": [100.0 + float(i) for i in range(rows)],
        "high": [101.0 + float(i) for i in range(rows)],
        "low": [99.0 + float(i) for i in range(rows)],
        "close": [100.5 + float(i) for i in range(rows)],
        "volume": [10.0 + float(i) for i in range(rows)],
    }
    return pd.DataFrame(datos)


def test_estrategia_macd_hist_inversion_acepta_histograma_escalar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """La estrategia debe interpretar correctamente valores escalares del histograma."""

    df = _build_dataframe()
    total_rows = len(df)

    def _fake_calcular_macd(frame: pd.DataFrame) -> tuple[float, float, float]:
        if len(frame) < total_rows:
            return (0.0, 0.0, -0.25)
        return (0.0, 0.0, 0.35)

    monkeypatch.setattr(
        "core.strategies.entry.estrategia_macd_hist_inversion.calcular_macd",
        _fake_calcular_macd,
    )

    resultado = estrategia_macd_hist_inversion(df)

    assert resultado["activo"] is True
    assert "Inversión" in resultado["mensaje"]


def test_estrategia_macd_hist_inversion_reporta_histograma_indisponible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Si no hay histograma disponible debe responder como inactiva."""

    df = _build_dataframe()

    def _fake_calcular_macd(_: pd.DataFrame) -> tuple[float, float, float | None]:
        return (0.0, 0.0, None)

    monkeypatch.setattr(
        "core.strategies.entry.estrategia_macd_hist_inversion.calcular_macd",
        _fake_calcular_macd,
    )

    resultado = estrategia_macd_hist_inversion(df)

    assert resultado["activo"] is False
    assert resultado["mensaje"] == "Histograma MACD no disponible"
