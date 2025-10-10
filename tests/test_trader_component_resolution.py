from __future__ import annotations

import importlib

import pytest

from core.trader.trader_lite import (
    ComponentResolutionError,
    TraderComponentFactories,
    TraderLite,
)
from tests.factories import DummyConfig, DummySupervisor


@pytest.mark.asyncio
async def test_component_factories_override_defaults() -> None:
    config = DummyConfig(spread_dynamic=True, max_spread_ratio=0.01)

    async def handler(_: dict) -> None:
        return None

    class FakeEventBus:
        pass

    class FakeOrderManager:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class FakeStrategyEngine:
        pass

    class FakePersistencia:
        pass

    class FakeSpreadGuard:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    def fake_sync_sim(_: object) -> None:
        fake_sync_sim.called = True

    fake_sync_sim.called = False

    def fake_verificar(*args, **kwargs):
        return {"ok": True}

    factories = TraderComponentFactories(
        event_bus=FakeEventBus,
        order_manager=FakeOrderManager,
        strategy_engine=FakeStrategyEngine,
        persistencia_tecnica=FakePersistencia,
        spread_guard=FakeSpreadGuard,
        verificar_entrada=fake_verificar,
        sync_sim=fake_sync_sim,
    )

    trader = TraderLite(
        config,
        candle_handler=handler,
        supervisor=DummySupervisor(),
        component_factories=factories,
    )

    assert trader._EventBus is FakeEventBus
    assert trader._OrderManager is FakeOrderManager
    assert trader._StrategyEngine is FakeStrategyEngine
    assert trader._PersistenciaTecnica is FakePersistencia
    assert trader._SpreadGuard is FakeSpreadGuard
    assert isinstance(trader.spread_guard, FakeSpreadGuard)
    assert trader._verificar_entrada is fake_verificar
    assert trader.get_component_error("verificar_entrada") is None

    await trader.stop()


def test_component_resolution_strict_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DummyConfig(trader_required_components={"verificar_entrada"})

    async def handler(_: dict) -> None:
        return None

    original_import = importlib.import_module

    def fake_import(name: str, package: str | None = None):
        if name == "core.strategies.entry.verificar_entradas":
            raise ImportError("boom")
        return original_import(name, package)

    monkeypatch.setattr(
        "core.trader.trader_lite.importlib.import_module",
        fake_import,
    )

    with pytest.raises(ComponentResolutionError):
        TraderLite(
            config,
            candle_handler=handler,
            supervisor=DummySupervisor(),
            strict_components=True,
        )
