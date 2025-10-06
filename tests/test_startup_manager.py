"""Pruebas unitarias para la lógica de sincronización del ``StartupManager``."""
from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from core.startup_manager import StartupManager


class _DummyTrader:
    """Trader mínimo que expone un ``data_feed`` configurable."""

    def __init__(self, feed: object) -> None:
        self.data_feed = feed


@pytest.mark.asyncio
async def test_wait_ws_with_asyncio_event() -> None:
    """``_wait_ws`` debe desbloquearse tan pronto se active ``asyncio.Event``."""

    event = asyncio.Event()
    feed = SimpleNamespace(ws_connected_event=event)
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _setter() -> None:
        await asyncio.sleep(0.05)
        event.set()

    asyncio.create_task(_setter())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_with_threading_event() -> None:
    """Permite coordinar feeds basados en threads mediante ``threading.Event``."""

    event = threading.Event()
    feed = SimpleNamespace(ws_connected_event=event)
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _setter() -> None:
        await asyncio.sleep(0.05)
        event.set()

    asyncio.create_task(_setter())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_timeout_without_signal() -> None:
    """Si la señal no llega antes del timeout se lanza ``RuntimeError``."""

    event = asyncio.Event()
    feed = SimpleNamespace(ws_connected_event=event)
    manager = StartupManager(trader=_DummyTrader(feed))

    with pytest.raises(RuntimeError, match="WS no conectado"):
        await manager._wait_ws(0.1)


@pytest.mark.asyncio
async def test_wait_ws_fails_fast_on_failure_event() -> None:
    """Si se emite la señal de error se aborta sin esperar al timeout."""

    success = asyncio.Event()
    failure = asyncio.Event()
    feed = SimpleNamespace(
        ws_connected_event=success,
        ws_failed_event=failure,
        ws_failure_reason="fallo handshake",
    )
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _fail_later() -> None:
        await asyncio.sleep(0.05)
        failure.set()

    asyncio.create_task(_fail_later())

    with pytest.raises(RuntimeError, match="fallo handshake"):
        await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_failure_event_pre_set() -> None:
    """La señal de error previa debe detener inmediatamente la espera."""

    success = asyncio.Event()
    failure = asyncio.Event()
    failure.set()
    feed = SimpleNamespace(
        ws_connected_event=success,
        ws_failed_event=failure,
        ws_failure_reason="host inalcanzable",
    )
    manager = StartupManager(trader=_DummyTrader(feed))

    with pytest.raises(RuntimeError, match="host inalcanzable"):
        await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_failure_with_threading_event() -> None:
    """Soporta señales de error provenientes de ``threading.Event``."""

    success = threading.Event()
    failure = threading.Event()
    feed = SimpleNamespace(
        ws_connected_event=success,
        ws_failed_event=failure,
        ws_failure_reason="fallo thread",
    )
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _trigger_failure() -> None:
        await asyncio.sleep(0.05)
        failure.set()

    asyncio.create_task(_trigger_failure())

    with pytest.raises(RuntimeError, match="fallo thread"):
        await manager._wait_ws(1.0)
