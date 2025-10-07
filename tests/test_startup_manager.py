"""Pruebas unitarias para la lógica de sincronización del ``StartupManager``."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core import startup_manager as startup_module
from core.startup_manager import StartupManager


class _DummyTrader:
    """Trader mínimo que expone un ``data_feed`` configurable."""

    def __init__(self, feed: object) -> None:
        self.data_feed = feed


class _CapturingLogger:
    """Logger mínimo que registra las llamadas recibidas para aserciones."""

    def __init__(self) -> None:
        self.info_calls: list[dict[str, Any]] = []
        self.warning_calls: list[dict[str, Any]] = []
        self.debug_calls: list[dict[str, Any]] = []
        self.error_calls: list[dict[str, Any]] = []

    def _record(self, storage: list[dict[str, Any]], message: str, *args: Any, **kwargs: Any) -> None:
        rendered = message % args if args else message
        storage.append({
            "message": rendered,
            "raw": message,
            "args": args,
            "kwargs": kwargs,
        })

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._record(self.info_calls, message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._record(self.warning_calls, message, *args, **kwargs)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._record(self.debug_calls, message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - seguridad
        self._record(self.error_calls, message, *args, **kwargs)


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


@pytest.mark.asyncio
async def test_wait_ws_success_event_pre_set() -> None:
    """Un feed debe desbloquear al instante si el éxito ya está señalado."""

    event = asyncio.Event()
    event.set()
    feed = SimpleNamespace(ws_connected_event=event)
    manager = StartupManager(trader=_DummyTrader(feed))

    await manager._wait_ws(0.1)


@pytest.mark.asyncio
async def test_wait_ws_success_threading_event_pre_set() -> None:
    """Compatibilidad con ``threading.Event`` preactivado."""

    event = threading.Event()
    event.set()
    feed = SimpleNamespace(ws_connected_event=event)
    manager = StartupManager(trader=_DummyTrader(feed))

    await manager._wait_ws(0.1)


@pytest.mark.asyncio
async def test_wait_ws_polling_property_activos() -> None:
    """Cuando no hay eventos recurre a la propiedad ``activos``."""

    class _PropertyFeed:
        def __init__(self) -> None:
            self._flag = False

        @property
        def activos(self) -> bool:
            return self._flag

    feed = _PropertyFeed()
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _activate() -> None:
        await asyncio.sleep(0.05)
        feed._flag = True

    asyncio.create_task(_activate())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_polling_callable_activos() -> None:
    """También acepta ``activos`` como método callable."""

    class _CallableFeed:
        def __init__(self) -> None:
            self._flag = False

        def activos(self) -> bool:
            return self._flag

    feed = _CallableFeed()
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _activate() -> None:
        await asyncio.sleep(0.05)
        feed._flag = True

    asyncio.create_task(_activate())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_load_config_reports_previous_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Debe registrar la presencia de un snapshot previo durante el arranque."""

    snapshot_path = tmp_path / "startup_snapshot.json"
    monkeypatch.setattr(startup_module, "SNAPSHOT_PATH", snapshot_path)

    data = {
        "symbols": ["BTCUSDT"],
        "modo_real": True,
        "timestamp": time.time() - 500,
    }
    snapshot_path.write_text(json.dumps(data))

    trader = _DummyTrader(SimpleNamespace())
    config = SimpleNamespace(symbols=["BTCUSDT"], modo_real=True)
    manager = StartupManager(trader=trader, config=config)
    manager._restart_alert_seconds = 60.0
    logger = _CapturingLogger()
    manager.log = logger

    await manager._load_config()

    assert logger.info_calls, "Se esperaba al menos un log informativo del snapshot"
    info_call = logger.info_calls[0]
    assert info_call["message"] == "Snapshot previo detectado"
    payload = info_call["kwargs"].get("extra", {}).get("startup_snapshot")
    assert payload is not None
    assert payload["symbols"] == ["BTCUSDT"]
    assert payload["modo_real"] is True
    assert payload["age_seconds"] and payload["age_seconds"] > 0

    assert not logger.warning_calls


@pytest.mark.asyncio
async def test_load_config_warns_on_fast_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Reinicios muy rápidos deben generar una advertencia para diagnóstico."""

    snapshot_path = tmp_path / "startup_snapshot.json"
    monkeypatch.setattr(startup_module, "SNAPSHOT_PATH", snapshot_path)

    data = {
        "symbols": ["ETHUSDT"],
        "modo_real": False,
        "timestamp": time.time() - 5,
    }
    snapshot_path.write_text(json.dumps(data))

    trader = _DummyTrader(SimpleNamespace())
    config = SimpleNamespace(symbols=["ETHUSDT"], modo_real=False)
    manager = StartupManager(trader=trader, config=config)
    manager._restart_alert_seconds = 60.0
    logger = _CapturingLogger()
    manager.log = logger

    await manager._load_config()

    assert logger.warning_calls, "Se esperaba advertencia por reinicio rápido"
    warning_call = logger.warning_calls[0]
    assert warning_call["message"].startswith("Reinicio detectado")
    payload = warning_call["kwargs"].get("extra", {}).get("startup_snapshot")
    assert payload is not None
    assert payload["symbols"] == ["ETHUSDT"]
    assert payload["modo_real"] is False
    assert payload["age_seconds"] < manager._restart_alert_seconds


@pytest.mark.asyncio
async def test_wait_ws_polling_is_active_method() -> None:
    """La ruta de fallback ``is_active()`` debe habilitar el flujo."""

    class _IsActiveFeed:
        def __init__(self) -> None:
            self._flag = False

        def is_active(self) -> bool:
            return self._flag

    feed = _IsActiveFeed()
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _activate() -> None:
        await asyncio.sleep(0.05)
        feed._flag = True

    asyncio.create_task(_activate())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_reloads_feed_reference_during_polling() -> None:
    """Debe detectar si el Trader actualiza ``data_feed`` dinámicamente."""

    initial_feed = SimpleNamespace(ws_connected_event=None)
    trader = _DummyTrader(initial_feed)
    manager = StartupManager(trader=trader)

    replacement_event = asyncio.Event()
    replacement_feed = SimpleNamespace(ws_connected_event=replacement_event)

    async def _swap_feed() -> None:
        await asyncio.sleep(0.05)
        trader.data_feed = replacement_feed
        replacement_event.set()

    asyncio.create_task(_swap_feed())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_degrades_thread_event_to_polling() -> None:
    """Si ``wait`` de un ``threading.Event`` falla, debe degradar a sondeo."""

    class _BrokenEvent:
        def wait(self, timeout: float | None = None) -> bool:  # pragma: no cover - simula fallo
            raise RuntimeError("thread wait incompatible")

    class _Feed:
        def __init__(self) -> None:
            self.ws_connected_event = _BrokenEvent()
            self._flag = False

        def activos(self) -> bool:
            return self._flag

    feed = _Feed()
    manager = StartupManager(trader=_DummyTrader(feed))

    async def _activate() -> None:
        await asyncio.sleep(0.05)
        feed._flag = True

    asyncio.create_task(_activate())

    await manager._wait_ws(1.0)


@pytest.mark.asyncio
async def test_wait_ws_failure_thread_event_timeout_results_in_error() -> None:
    """Un ``threading.Event`` de fallo que no se activa provoca timeout final."""

    class _TimeoutEvent(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            super().wait(timeout=timeout)
            return False

    failure = _TimeoutEvent()
    feed = SimpleNamespace(ws_connected_event=None, ws_failed_event=failure)
    manager = StartupManager(trader=_DummyTrader(feed))

    with pytest.raises(RuntimeError, match="WS no conectado"):
        await manager._wait_ws(0.2)


@pytest.mark.asyncio
async def test_wait_ws_managed_feed_uses_event_bus_success() -> None:
    """Feeds gestionados por el Trader utilizan ``event_bus.wait``."""

    class _Bus:
        async def wait(self, event_name: str) -> None:
            assert event_name == "datafeed_connected"
            await asyncio.sleep(0.05)

    feed = SimpleNamespace(_managed_by_trader=True)
    trader = _DummyTrader(feed)
    trader.event_bus = _Bus()
    manager = StartupManager(trader=trader)

    await manager._wait_ws(0.5)


@pytest.mark.asyncio
async def test_wait_ws_managed_feed_event_bus_timeout() -> None:
    """Si el bus no responde a tiempo se propaga ``RuntimeError``."""

    class _Bus:
        async def wait(self, event_name: str) -> None:  # pragma: no cover - espera excesiva
            assert event_name == "datafeed_connected"
            await asyncio.sleep(1.0)

    feed = SimpleNamespace(_managed_by_trader=True)
    trader = _DummyTrader(feed)
    trader.event_bus = _Bus()
    manager = StartupManager(trader=trader)

    with pytest.raises(RuntimeError, match="WS no conectado"):
        await manager._wait_ws(0.1)
