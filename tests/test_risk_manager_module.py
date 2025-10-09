from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from core.risk.risk_manager import RiskManager


class DummyCapitalManager:
    def __init__(self, capital: dict[str, float], *, libre: bool = True) -> None:
        self.capital_por_simbolo = capital
        self._libre = libre

    def hay_capital_libre(self) -> bool:
        return self._libre


@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_actualizar(symbol: str, perdida: float) -> None:
        return None

    monkeypatch.setattr("core.risk.risk_manager.actualizar_perdida", fake_actualizar)

    dummy_reporter = SimpleNamespace(ultimas_operaciones={})
    monkeypatch.setattr("core.risk.risk_manager.reporter_diario", dummy_reporter)


def test_registrar_perdida_activa_cooldown() -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    manager = RiskManager(0.05, capital_manager=capital, cooldown_pct=0.1, cooldown_duracion=60)

    manager.registrar_perdida("BTCUSDT", -20.0)

    assert pytest.approx(manager.riesgo_diario, rel=1e-9) == 20.0
    assert manager.cooldown_activo is True


def test_permite_entrada_verifica_condiciones() -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0}, libre=False)
    manager = RiskManager(0.05, capital_manager=capital)
    manager._cooldown_fin = datetime.now(UTC) + timedelta(seconds=10)

    assert manager.permite_entrada("ETHUSDT", {}, 0.5) is False

    manager._cooldown_fin = None
    assert manager.permite_entrada("ETHUSDT", {}, 0.5) is False

    capital._libre = True
    manager.abrir_posicion("BTCUSDT")
    manager.abrir_posicion("ETHUSDT")
    correlaciones = {"BTCUSDT": 0.9}
    assert manager.permite_entrada("ETHUSDT", correlaciones, 0.5) is False

    correlaciones = {"BTCUSDT": 0.1}
    assert manager.permite_entrada("ETHUSDT", correlaciones, 0.5) is True


def test_ajustar_umbral_aplica_reglas() -> None:
    manager = RiskManager(0.04)

    manager.ajustar_umbral({"ganancia_semana": 0.1})
    assert manager.umbral > 0.04

    manager.ajustar_umbral({"drawdown": -0.1})
    assert manager.umbral <= 0.5

    manager.ajustar_umbral(
        {
            "winrate": 0.7,
            "capital_actual": 120.0,
            "capital_inicial": 100.0,
        }
    )
    assert manager.umbral <= 0.06

    manager.ajustar_umbral({"volatilidad_market": 3.0, "volatilidad_media": 1.0})
    manager.ajustar_umbral({"exposicion_actual": 0.6})
    manager.ajustar_umbral({"correlacion_media": 0.9})
    assert manager.umbral >= 0.01


def test_factor_volatilidad_reduce_exceso() -> None:
    manager = RiskManager(0.05)
    assert manager.factor_volatilidad(1.0, 1.0) == 1.0
    factor = manager.factor_volatilidad(5.0, 1.0, umbral=2.0)
    assert 0.25 <= factor <= 1.0


def test_multiplicador_kelly_media_suavizada(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_reporter = SimpleNamespace(
        ultimas_operaciones={
            "BTC": [{"retorno_total": 0.05}, {"retorno_total": 0.1}],
            "ETH": [{"retorno_total": -0.02}],
        }
    )
    monkeypatch.setattr("core.risk.risk_manager.reporter_diario", dummy_reporter)
    manager = RiskManager(0.05)

    factor1 = manager.multiplicador_kelly(n_trades=3)
    assert 0.5 <= factor1 <= 1.5

    dummy_reporter.ultimas_operaciones["BTC"].append({"retorno_total": 0.2})
    factor2 = manager.multiplicador_kelly(n_trades=3)
    assert factor1 <= factor2 <= 1.5


@pytest.mark.asyncio
async def test_kill_switch_cierra_posiciones_y_notifica() -> None:
    class DummyBus:
        def __init__(self) -> None:
            self.events: list[tuple[str, Any]] = []

        def subscribe(self, *_: Any, **__: Any) -> None:
            return None

        async def publish(self, event_type: str, data: Any) -> None:
            self.events.append((event_type, data))

    class DummyOrder:
        def __init__(self, precio: float) -> None:
            self.precio_entrada = precio

    class DummyOrderManager:
        def __init__(self) -> None:
            self.ordenes = {"BTCUSDT": DummyOrder(10.0)}
            self.closed: list[tuple[str, float, str]] = []

        async def cerrar_async(self, symbol: str, precio: float, motivo: str) -> None:
            self.closed.append((symbol, precio, motivo))

    bus = DummyBus()
    manager = RiskManager(0.05, bus=bus)

    orders = DummyOrderManager()
    triggered = await manager.kill_switch(orders, -0.1, -0.05, 3, 2)

    assert triggered is True
    assert orders.closed == [("BTCUSDT", 10.0, "Kill Switch")]
    assert bus.events and bus.events[0][0] == "notify"
