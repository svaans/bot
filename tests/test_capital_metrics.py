from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.capital_manager import CapitalManager
from core.capital_repository import CapitalRepository
from observability.metrics import (
    CAPITAL_CONFIGURED_GAUGE,
    CAPITAL_CONFIGURED_TOTAL,
    CAPITAL_DIVERGENCE_ABSOLUTE,
    CAPITAL_DIVERGENCE_RATIO,
    CAPITAL_DIVERGENCE_THRESHOLD,
    CAPITAL_REGISTERED_GAUGE,
    CAPITAL_REGISTERED_TOTAL,
)


def _reset_capital_metrics(symbols: tuple[str, ...]) -> None:
    for symbol in symbols:
        CAPITAL_REGISTERED_GAUGE.labels(symbol=symbol).set(0.0)
        CAPITAL_CONFIGURED_GAUGE.labels(symbol=symbol).set(0.0)
    CAPITAL_REGISTERED_TOTAL.set(0.0)
    CAPITAL_CONFIGURED_TOTAL.set(0.0)
    CAPITAL_DIVERGENCE_ABSOLUTE.set(0.0)
    CAPITAL_DIVERGENCE_RATIO.set(0.0)
    CAPITAL_DIVERGENCE_THRESHOLD.set(0.0)


class _DummyBus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        self.events.append((event, payload))


class _Config:
    symbols = ["BTCUSDT", "ETHUSDT"]
    risk_capital_total = 200.0
    risk_capital_default_per_symbol = 0.0
    risk_capital_per_symbol = {"BTCUSDT": 120.0, "ETHUSDT": 80.0}
    risk_kelly_base = 0.1
    min_order_eur = 10.0
    risk_capital_divergence_threshold = 0.25


def test_capital_metrics_and_alerts(tmp_path: Path) -> None:
    symbols = ("BTCUSDT", "ETHUSDT")
    _reset_capital_metrics(symbols)

    repo = CapitalRepository(path=tmp_path / "capital.json")
    manager = CapitalManager(_Config(), capital_repository=repo)

    for symbol in symbols:
        assert CAPITAL_REGISTERED_GAUGE.labels(symbol=symbol)._value == pytest.approx(
            _Config.risk_capital_per_symbol[symbol]
        )
        assert CAPITAL_CONFIGURED_GAUGE.labels(symbol=symbol)._value == pytest.approx(
            _Config.risk_capital_per_symbol[symbol]
        )

    assert CAPITAL_REGISTERED_TOTAL._value == pytest.approx(200.0)
    assert CAPITAL_CONFIGURED_TOTAL._value == pytest.approx(200.0)
    assert CAPITAL_DIVERGENCE_RATIO._value == pytest.approx(0.0)
    assert CAPITAL_DIVERGENCE_THRESHOLD._value == pytest.approx(
        _Config.risk_capital_divergence_threshold
    )

    bus = _DummyBus()
    manager.event_bus = bus

    manager.actualizar_exposure("BTCUSDT", 10.0)

    assert CAPITAL_REGISTERED_GAUGE.labels(symbol="BTCUSDT")._value == pytest.approx(10.0)
    assert CAPITAL_REGISTERED_TOTAL._value == pytest.approx(90.0)

    expected_ratio = abs(90.0 - 200.0) / 200.0
    assert CAPITAL_DIVERGENCE_RATIO._value == pytest.approx(expected_ratio)
    assert any(event == "capital.divergence_detected" for event, _ in bus.events)
    detected_payload = next(payload for event, payload in bus.events if event == "capital.divergence_detected")
    assert detected_payload["status"] == "detected"
    assert detected_payload["threshold"] == pytest.approx(_Config.risk_capital_divergence_threshold)

    manager.actualizar_exposure("BTCUSDT", 120.0)

    assert CAPITAL_REGISTERED_TOTAL._value == pytest.approx(200.0)
    assert CAPITAL_DIVERGENCE_RATIO._value == pytest.approx(0.0)
    assert any(event == "capital.divergence_cleared" for event, _ in bus.events)

    _reset_capital_metrics(symbols)
