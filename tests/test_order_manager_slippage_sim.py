"""Slippage simulado (BACKTEST_SLIPPAGE_BPS) y helpers de ejecución."""
from __future__ import annotations

import pytest

from core.orders.order_manager import (
    _backtest_slippage_bps,
    _sim_aplicar_slippage_entrada_salida,
)


def test_backtest_slippage_bps_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BACKTEST_SLIPPAGE_BPS", raising=False)
    assert _backtest_slippage_bps() == 0.0


def test_sim_slippage_long_entrada_salida(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_SLIPPAGE_BPS", "100")
    assert _sim_aplicar_slippage_entrada_salida(100.0, "long", es_entrada=True) == pytest.approx(101.0)
    assert _sim_aplicar_slippage_entrada_salida(100.0, "long", es_entrada=False) == pytest.approx(99.0)


def test_sim_slippage_short_entrada_salida(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_SLIPPAGE_BPS", "100")
    assert _sim_aplicar_slippage_entrada_salida(100.0, "short", es_entrada=True) == pytest.approx(99.0)
    assert _sim_aplicar_slippage_entrada_salida(100.0, "venta", es_entrada=False) == pytest.approx(101.0)
