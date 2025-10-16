from __future__ import annotations

import datetime as dt
import time
from collections import deque
from typing import Any, Callable, Sequence, Tuple

import pandas as pd
import pytest

from core.procesar_vela import procesar_vela
from core.trader_modular import Trader
from data_feed import DataFeed
from tests.factories import DummyConfig, DummySupervisor


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--flags",
        action="append",
        dest="feature_flags",
        default=None,
        help=(
            "Ejecuta únicamente las pruebas marcadas con los feature flags dados. "
            "Separar múltiples flags con comas."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "flags(*names): marca pruebas canarias asociadas a uno o más feature flags.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    raw_requested = config.getoption("feature_flags")
    if not raw_requested:
        return

    requested: set[str] = set()
    for chunk in raw_requested:
        if not chunk:
            continue
        for flag in chunk.split(","):
            normalized = flag.strip().lower()
            if normalized:
                requested.add(normalized)

    if not requested:
        return

    skip_marker = pytest.mark.skip(reason="no incluido en los feature flags solicitados")

    for item in items:
        item_flags: set[str] = set()
        for marker in item.iter_markers(name="flags"):
            for value in marker.args:
                if isinstance(value, str):
                    item_flags.add(value.lower())

        if not item_flags.intersection(requested):
            item.add_marker(skip_marker)


@pytest.fixture(autouse=True)
def stub_data_feed(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Reemplaza ``DataFeed`` por un stub liviano durante los tests."""

    instances: list[Any] = []

    class StubFeed:
        def __init__(self, intervalo: str, **kwargs: Any) -> None:
            self.intervalo = intervalo
            self.kwargs = kwargs
            self.handler_timeout = kwargs.get("handler_timeout")
            self.inactivity_intervals = kwargs.get("inactivity_intervals")
            self.queue_max = kwargs.get("queue_max")
            self.queue_min_recommended = kwargs.get("queue_min_recommended")
            self.queue_policy = kwargs.get("queue_policy")
            self.monitor_interval = kwargs.get("monitor_interval")
            self.backpressure = kwargs.get("backpressure")
            self.cancel_timeout = kwargs.get("cancel_timeout")
            self._managed_by_trader = kwargs.get("_managed_by_trader", False)
            self.detener_calls = 0
            self.precargar_calls: list[dict[str, Any]] = []
            self.escuchar_calls: deque[dict[str, Any]] = deque()
            instances.append(self)

        async def detener(self) -> None:
            self.detener_calls += 1

        async def escuchar(
            self,
            symbols: list[str] | tuple[str, ...],
            handler: Callable[[dict], Any],
            *,
            cliente: Any | None = None,
        ) -> None:
            self.escuchar_calls.append(
                {
                    "symbols": tuple(symbols),
                    "handler": handler,
                    "cliente": cliente,
                }
            )

        async def precargar(
            self,
            symbols: list[str] | tuple[str, ...],
            *,
            cliente: Any | None = None,
            minimo: int | None = None,
        ) -> None:
            self.precargar_calls.append(
                {
                    "symbols": tuple(symbols),
                    "cliente": cliente,
                    "minimo": minimo,
                }
            )

    monkeypatch.setattr("core.trader_modular.DataFeed", StubFeed)
    return instances


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, Callable[[float], float]]:
    """Congela ``time.time`` y ``datetime.datetime.now`` para escenarios deterministas."""

    current = time.time()

    def _time() -> float:
        return current

    def advance(delta: float) -> float:
        nonlocal current
        current += float(delta)
        return current

    monkeypatch.setattr(time, "time", _time)

    real_datetime = dt.datetime

    class FrozenDatetime(real_datetime):  # type: ignore[misc]
        @classmethod
        def now(cls, tz: dt.tzinfo | None = None) -> "FrozenDatetime":
            if tz is None:
                return cls.fromtimestamp(current)
            return cls.fromtimestamp(current, tz)

        @classmethod
        def utcnow(cls) -> "FrozenDatetime":
            return cls.fromtimestamp(current, dt.timezone.utc)

    monkeypatch.setattr(dt, "datetime", FrozenDatetime)

    return {"advance": advance, "now": lambda: current}


@pytest.fixture
def trader_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[
    [Sequence[str]], Trader
]:
    """Crea traders reales pero con dependencias mínimas para los tests."""

    class _SilentEventBus:
        def __init__(self) -> None:
            self.emitted: list[Tuple[str, Any | None]] = []

        def emit(self, event_type: str, data: Any | None = None) -> None:  # pragma: no cover - stub
            self.emitted.append((event_type, data))

        async def publish(self, event_type: str, data: Any | None) -> None:  # pragma: no cover - stub
            self.emitted.append((event_type, data))

        async def close(self) -> None:  # pragma: no cover - stub
            return None

    monkeypatch.setattr("core.event_bus.EventBus", _SilentEventBus, raising=False)

    class _DummyOrders:
        def __init__(self) -> None:
            self.created: list[dict[str, Any]] = []

        def obtener(self, _symbol: str) -> None:
            return None

        def crear(self, **payload: Any) -> None:
            self.created.append(dict(payload))

    async def _pipeline(
        self: Trader,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        *,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        if on_event is not None:
            on_event(
                "entry_probe",
                {"symbol": symbol, "buffer_len": len(df), "timeframe": getattr(df, "tf", None)},
            )
        close_price = float(df.iloc[-1]["close"])
        return {
            "symbol": symbol,
            "side": "long",
            "precio_entrada": close_price,
            "stop_loss": close_price * 0.99,
            "take_profit": close_price * 1.01,
            "score": 1.0,
        }

    def _factory(
        symbols: Sequence[str] = ("BTCUSDT",),
        *,
        timeframe: str = "5m",
        fastpath: bool = False,
    ) -> Trader:
        normalized = [s.upper() for s in symbols]
        config = DummyConfig(symbols=normalized, intervalo_velas=timeframe)
        config.min_bars = 1
        if fastpath:
            config.trader_fastpath_enabled = True
            config.trader_fastpath_skip_entries = True
            config.trader_fastpath_threshold = 2
            config.trader_fastpath_resume_threshold = 1
        else:
            config.trader_fastpath_enabled = False
            config.trader_fastpath_skip_entries = False
            config.trader_fastpath_threshold = 400
            config.trader_fastpath_resume_threshold = 350

        trader = Trader(config, candle_handler=procesar_vela, supervisor=DummySupervisor())
        trader.habilitar_estrategias()
        trader._backfill_service = None
        trader._backfill_enabled = False
        trader.min_bars = 1
        trader.orders = _DummyOrders()
        trader._verificar_entrada = _pipeline.__get__(trader, Trader)
        trader._verificar_entrada_provider = "unit-test"
        trader.bus = None
        trader.event_bus = None
        return trader

    return _factory


@pytest.fixture
def trader_instance(trader_factory: Callable[[Sequence[str]], Trader]) -> Trader:
    return trader_factory()


@pytest.fixture
def datafeed_instance() -> DataFeed:
    """Instancia real de ``DataFeed`` para las pruebas de integración."""

    return DataFeed("5m", handler_timeout=0.5, monitor_interval=0.1, queue_max=16, backpressure=False)
