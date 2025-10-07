"""Pruebas de extremo a extremo para la señalización de conexión WebSocket."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, Callable, List

import pytest

from config.development import DevelopmentConfig
from core.event_bus import EventBus
from core.startup_manager import StartupManager
from core.trader.trader import Trader
from core.trader import trader_lite as trader_lite_mod


class _StubFeed:
    """Feed mínimo que permite controlar las señales de conexión en tests."""

    instances: List["_StubFeed"] = []

    def __init__(self, intervalo: str, *, on_event: Callable[[str, dict], None] | None = None, **_: Any) -> None:
        self.intervalo = intervalo
        self.on_event = on_event
        self.ws_connected_event = asyncio.Event()
        self.ws_failed_event = asyncio.Event()
        self.ws_failure_reason: str | None = None
        self._stop = asyncio.Event()
        self._handler: Callable[[dict], asyncio.Future | asyncio.Task | None] | None = None
        self._symbols: List[str] = []
        self.started = asyncio.Event()
        self._active = False
        self._managed_by_trader = True
        _StubFeed.instances.append(self)

    @property
    def activos(self) -> bool:
        return self._active

    async def escuchar(self, symbols: List[str], handler: Callable[[dict], Any], *, cliente: Any | None = None) -> None:
        del cliente
        self._symbols = list(symbols)
        self._handler = handler
        self.started.set()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:  # pragma: no cover - compatibilidad cancelación
            raise

    async def detener(self) -> None:
        self._stop.set()
        await asyncio.sleep(0)

    async def push_candle(self, candle: dict) -> None:
        if self._handler is None:
            raise RuntimeError("Handler no inicializado todavía")
        await self._handler(candle)

    def signal_connected(self) -> None:
        self._active = True
        if not self.ws_connected_event.is_set():
            self.ws_connected_event.set()
            if self.on_event is not None:
                payload = {"intervalo": self.intervalo}
                if self._symbols:
                    payload["symbols"] = list(self._symbols)
                self.on_event("ws_connected", payload)

    def signal_failure(self, reason: str) -> None:
        self.ws_failure_reason = reason
        if not self.ws_failed_event.is_set():
            self.ws_failed_event.set()
            if self.on_event is not None:
                self.on_event("ws_connect_failed", {"intervalo": self.intervalo, "reason": reason})


@pytest.fixture(autouse=True)
def _stub_data_feed(monkeypatch: pytest.MonkeyPatch) -> type[_StubFeed]:
    """Sustituye el DataFeed real por un stub controlable durante cada prueba."""

    _StubFeed.instances.clear()
    monkeypatch.setattr(trader_lite_mod, "_data_feed_cls", lambda: _StubFeed)
    return _StubFeed


def _build_config() -> DevelopmentConfig:
    base = DevelopmentConfig()
    return replace(base, symbols=["BTCUSDT"], intervalo_velas="1m")


async def _start_trader() -> tuple[Trader, _StubFeed]:
    config = _build_config()
    trader = Trader(config)
    if getattr(trader, "event_bus", None) is None:
        bus = EventBus()
        trader.bus = bus
        trader.event_bus = bus
    trader.start()
    feed = trader.feed
    assert isinstance(feed, _StubFeed)
    await asyncio.wait_for(feed.started.wait(), timeout=1.0)
    return trader, feed


async def _stop_trader(trader: Trader) -> None:
    bus = getattr(trader, "event_bus", None)
    await trader.stop()
    if bus is not None and hasattr(bus, "close"):
        await bus.close()


@pytest.mark.asyncio
async def test_traderlite_emite_evento_bus_al_conectarse_ws() -> None:
    trader, feed = await _start_trader()
    try:
        feed.signal_connected()
        payload = await asyncio.wait_for(trader.event_bus.wait("datafeed_connected"), timeout=0.5)
        assert payload is not None
        assert payload.get("symbols") == ["BTCUSDT"]
    finally:
        await _stop_trader(trader)


@pytest.mark.asyncio
async def test_traderlite_evento_bus_incluye_symbol_de_primera_vela() -> None:
    trader, feed = await _start_trader()
    try:
        candle = {"symbol": "BTCUSDT", "timestamp": 1700000000000, "is_closed": True}
        await feed.push_candle(candle)
        payload = await asyncio.wait_for(trader.event_bus.wait("datafeed_connected"), timeout=0.5)
        assert payload["symbol"] == "BTCUSDT"
    finally:
        await _stop_trader(trader)


@pytest.mark.asyncio
async def test_traderlite_evento_bus_emite_una_sola_vez() -> None:
    trader, feed = await _start_trader()
    try:
        feed.signal_connected()
        payload = await asyncio.wait_for(trader.event_bus.wait("datafeed_connected"), timeout=0.5)
        assert payload is not None
        assert getattr(trader, "_datafeed_connected_emitted", False) is True
    finally:
        await _stop_trader(trader)


@pytest.mark.asyncio
async def test_traderlite_evento_bus_timeout_sin_senal() -> None:
    trader, _ = await _start_trader()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(trader.event_bus.wait("datafeed_connected"), timeout=0.1)
    finally:
        await _stop_trader(trader)


async def _prepare_manager(ws_timeout: float = 0.5) -> tuple[StartupManager, Trader, _StubFeed]:
    trader, feed = await _start_trader()
    manager = StartupManager(trader=trader, ws_timeout=ws_timeout)
    await manager._open_streams()
    return manager, trader, feed


async def _cleanup_manager(manager: StartupManager, trader: Trader) -> None:
    try:
        await manager._stop_streams()
    finally:
        await _stop_trader(trader)


@pytest.mark.asyncio
async def test_wait_ws_con_trader_gestionado_recibe_senal_de_bus() -> None:
    manager, trader, feed = await _prepare_manager()
    try:
        feed.signal_connected()
        await manager._wait_ws(1.0)
    finally:
        await _cleanup_manager(manager, trader)


@pytest.mark.asyncio
async def test_wait_ws_timeout_si_no_llega_senal() -> None:
    manager, trader, _ = await _prepare_manager(ws_timeout=0.2)
    try:
        with pytest.raises(RuntimeError, match="WS no conectado"):
            await manager._wait_ws(0.2)
    finally:
        await _cleanup_manager(manager, trader)


@pytest.mark.asyncio
async def test_wait_ws_propagates_failure_reason() -> None:
    manager, trader, feed = await _prepare_manager(ws_timeout=0.5)
    try:
        original_bus = getattr(trader, "event_bus", None)
        if original_bus is not None and hasattr(original_bus, "close"):
            await original_bus.close()
        trader.event_bus = None
        trader.bus = None
        manager.event_bus = None
        feed.signal_failure("timeout backfill")
        with pytest.raises(RuntimeError, match="timeout backfill"):
            await manager._wait_ws(0.5)
    finally:
        await _cleanup_manager(manager, trader)


@pytest.mark.asyncio
async def test_wait_ws_funciona_sin_event_bus() -> None:
    manager, trader, feed = await _prepare_manager()
    try:
        original_bus = getattr(trader, "event_bus", None)
        if original_bus is not None and hasattr(original_bus, "close"):
            await original_bus.close()
        trader.event_bus = None
        trader.bus = None
        manager.event_bus = None
        feed.signal_connected()
        await manager._wait_ws(1.0)
    finally:
        await _cleanup_manager(manager, trader)


@pytest.mark.asyncio
async def test_wait_ws_detecta_actividad_por_polling() -> None:
    manager, trader, feed = await _prepare_manager(ws_timeout=0.5)
    try:
        original_bus = getattr(trader, "event_bus", None)
        if original_bus is not None and hasattr(original_bus, "close"):
            await original_bus.close()
        trader.event_bus = None
        trader.bus = None
        manager.event_bus = None
        feed.ws_connected_event = object()  # fuerza degradación a polling
        feed.ws_failed_event = object()

        async def _activate() -> None:
            await asyncio.sleep(0.05)
            feed._active = True

        asyncio.create_task(_activate())
        await manager._wait_ws(0.5)
    finally:
        await _cleanup_manager(manager, trader)


@pytest.mark.asyncio
async def test_wait_ws_detecta_reemplazo_de_feed_en_polling() -> None:
    manager, trader, feed = await _prepare_manager(ws_timeout=0.5)
    try:
        original_bus = getattr(trader, "event_bus", None)
        if original_bus is not None and hasattr(original_bus, "close"):
            await original_bus.close()
        trader.event_bus = None
        trader.bus = None
        manager.event_bus = None
        feed.ws_connected_event = object()
        feed.ws_failed_event = object()

        async def _swap_feed() -> None:
            await asyncio.sleep(0.05)
            nuevo = _StubFeed(feed.intervalo)
            nuevo._active = True
            nuevo.signal_connected()
            trader.feed = nuevo
            trader.data_feed = nuevo

        waiter = asyncio.create_task(manager._wait_ws(0.5))
        asyncio.create_task(_swap_feed())
        await waiter
    finally:
        await _cleanup_manager(manager, trader)
