"""Pruebas unitarias para la lógica de sincronización del ``StartupManager``."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from core import startup_manager as startup_module
from core.startup_manager import StartupManager
from core.persistencia_tecnica import PersistenciaTecnica


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


class _TestOpenStreamsError(RuntimeError):
    """Error deliberado para simular fallos durante ``_open_streams``."""


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


def test_snapshot_includes_persistencia_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """El snapshot debe incluir el estado técnico cuando está disponible."""

    snapshot_path = tmp_path / "startup_snapshot.json"
    monkeypatch.setattr(startup_module, "SNAPSHOT_PATH", snapshot_path)

    config = SimpleNamespace(symbols=["BTCUSDT"], modo_real=False)
    persistencia = PersistenciaTecnica(minimo=2, peso_extra=0.75)
    persistencia.actualizar("BTCUSDT", {"long": True})
    trader = SimpleNamespace(config=config, persistencia=persistencia)

    manager = StartupManager(trader=trader, config=config)
    manager._snapshot()

    data = json.loads(snapshot_path.read_text())
    estado = data.get("persistencia_tecnica")
    assert estado is not None
    assert estado["minimo"] == 2
    assert estado["peso_extra"] == pytest.approx(0.75)
    assert estado["conteo"]["BTCUSDT"]["long"] == 1


def test_restore_persistencia_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Debe restaurar el estado técnico previo en el trader actual."""

    snapshot_path = tmp_path / "startup_snapshot.json"
    monkeypatch.setattr(startup_module, "SNAPSHOT_PATH", snapshot_path)

    persistencia_prev = PersistenciaTecnica(minimo=3, peso_extra=1.25)
    persistencia_prev.actualizar("BTCUSDT", {"long": True})
    persistencia_prev.actualizar("BTCUSDT", {"long": True})
    snapshot_payload = {
        "symbols": ["BTCUSDT"],
        "modo_real": False,
        "timestamp": time.time(),
        "persistencia_tecnica": persistencia_prev.export_state(),
    }
    snapshot_path.write_text(json.dumps(snapshot_payload))

    config = SimpleNamespace(symbols=["BTCUSDT"], modo_real=False)
    trader = SimpleNamespace(config=config, persistencia=PersistenciaTecnica())

    manager = StartupManager(trader=trader, config=config)
    manager._inspect_previous_snapshot()
    manager._restore_persistencia_state()

    assert trader.persistencia.minimo == 3
    assert trader.persistencia.peso_extra == pytest.approx(1.25)
    assert trader.persistencia.conteo == {"BTCUSDT": {"long": 2}}


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


@pytest.mark.asyncio
async def test_wait_ws_grace_fallback_invokes_ensure_running() -> None:
    """La ventana de gracia dispara ``ensure_running`` cuando no hay actividad."""

    class _Feed:
        def __init__(self) -> None:
            self.ws_connected_event = asyncio.Event()
            self.ws_failed_event = asyncio.Event()
            self._managed_by_trader = True
            self.connected = False
            self.ensure_running_calls = 0

        async def ensure_running(self) -> None:
            self.ensure_running_calls += 1
            await asyncio.sleep(0)
            self.connected = True
            self.ws_connected_event.set()

    feed = _Feed()
    manager = StartupManager(trader=_DummyTrader(feed), config={"ws_managed_by_trader": True})

    await manager._wait_ws(0.4, grace_for_trader=0.05)

    assert feed.ensure_running_calls == 1
    assert feed.ws_connected_event.is_set() is True
    
@pytest.mark.asyncio
async def test_run_rolls_back_streams_when_open_streams_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ante un fallo en ``_open_streams`` se debe invocar ``_stop_streams`` en el rollback."""

    trader = _DummyTrader(SimpleNamespace())
    manager = StartupManager(trader=trader, config=SimpleNamespace())
    manager.log = _CapturingLogger()

    load_config = AsyncMock()
    bootstrap = AsyncMock()
    validate_feeds = AsyncMock()
    stop_trader = AsyncMock()
    stop_streams = AsyncMock()

    monkeypatch.setattr(manager, "_load_config", load_config)
    monkeypatch.setattr(manager, "_bootstrap", bootstrap)
    monkeypatch.setattr(manager, "_validate_feeds", validate_feeds)
    monkeypatch.setattr(manager, "_stop_trader", stop_trader)
    monkeypatch.setattr(manager, "_stop_streams", stop_streams)
    monkeypatch.setattr(manager, "_open_streams", AsyncMock(side_effect=_TestOpenStreamsError("boom")))

    with pytest.raises(_TestOpenStreamsError):
        await manager.run()

    assert stop_streams.await_count == 1
    assert stop_trader.await_count == 1


@pytest.mark.asyncio
async def test_wait_ws_grace_detects_activity_without_fallback() -> None:
    """Si el Trader muestra actividad, no se fuerza ``ensure_running``."""

    class _Feed:
        def __init__(self) -> None:
            self.ws_connected_event = asyncio.Event()
            self.ws_failed_event = asyncio.Event()
            self._managed_by_trader = True
            self.connected = False
            self.ensure_running_calls = 0

        async def ensure_running(self) -> None:
            self.ensure_running_calls += 1

        def mark_connected(self) -> None:
            self.connected = True
            self.ws_connected_event.set()

    feed = _Feed()
    manager = StartupManager(trader=_DummyTrader(feed), config={"ws_managed_by_trader": True})

    async def _connect() -> None:
        await asyncio.sleep(0.02)
        feed.mark_connected()

    asyncio.create_task(_connect())

    await manager._wait_ws(0.5, grace_for_trader=0.1)

    assert feed.ensure_running_calls == 0
    assert feed.ws_connected_event.is_set() is True
    if hasattr(manager.config, "get"):
        assert manager.config.get("ws_managed_by_trader") is True
    else:
        assert getattr(manager.config, "ws_managed_by_trader", None) is True


@pytest.mark.asyncio
async def test_open_streams_runs_sync_start_on_loop_thread() -> None:
    """El trader síncrono debe ejecutarse en el mismo hilo del event loop."""

    loop_thread = threading.get_ident()

    class _Bus:
        async def wait(self, event_name: str) -> None:
            assert event_name == "datafeed_connected"

    class _Trader:
        def __init__(self) -> None:
            self.thread_id: int | None = None
            self.started = False
            self.data_feed = SimpleNamespace(_managed_by_trader=True)
            self.event_bus = _Bus()

        def ejecutar(self) -> None:
            asyncio.get_running_loop()
            self.thread_id = threading.get_ident()
            self.started = True

    trader = _Trader()
    manager = StartupManager(trader=trader)

    try:
        await manager._open_streams()
        await asyncio.sleep(0)

        assert trader.started is True
        assert trader.thread_id == loop_thread
    finally:
        await manager._stop_streams()

@pytest.mark.asyncio
async def test_open_streams_autostarts_managed_feed_without_wait() -> None:
    """Si el feed es gestionado pero el bus no soporta wait() se arranca como fallback."""

    class _Feed:
        def __init__(self) -> None:
            self._managed_by_trader = True
            self.start_calls = 0

        async def start(self) -> None:
            self.start_calls += 1
            await asyncio.sleep(0)

    class _Trader:
        def __init__(self, feed: _Feed) -> None:
            self.data_feed = feed
            self.event_bus = SimpleNamespace()  # sin wait()
            self.started = asyncio.Event()

        async def ejecutar(self) -> None:
            self.started.set()
            await asyncio.sleep(10)

    feed = _Feed()
    trader = _Trader(feed)
    manager = StartupManager(trader=trader)
    manager.log = _CapturingLogger()

    await manager._open_streams()
    await asyncio.wait_for(trader.started.wait(), timeout=0.5)
    await asyncio.sleep(0)

    assert feed.start_calls == 1
    assert any(
        call["message"]
        == "DataFeed gestionado pero sin soporte de EventBus.wait(); se arranca como fallback."
        for call in manager.log.warning_calls
    )

    await manager._stop_streams()


@pytest.mark.asyncio
async def test_open_streams_fallback_disables_managed_flag() -> None:
    """El fallback debe marcar el feed como autogestionado por el StartupManager."""

    class _Feed:
        def __init__(self) -> None:
            self._managed_by_trader = True
            self.start_calls = 0
            self.ws_connected_event = asyncio.Event()

        def start(self) -> None:
            self.start_calls += 1
            self.ws_connected_event.set()

    feed = _Feed()

    class _Trader:
        def __init__(self, df: _Feed) -> None:
            self.data_feed = df

        async def ejecutar(self) -> None:
            await asyncio.sleep(0)

    manager = StartupManager(
        trader=_Trader(feed),
        config={"ws_managed_by_trader": True},
    )

    try:
        await manager._open_streams()

        assert feed.start_calls == 1
        assert getattr(feed, "_managed_by_trader", None) is False
        if hasattr(manager.config, "get"):
            assert manager.config.get("ws_managed_by_trader") is False
        else:
            assert getattr(manager.config, "ws_managed_by_trader", None) is False
    finally:
        await manager._stop_streams()
@pytest.mark.asyncio
async def test_open_streams_fallback_emits_datafeed_connected_event() -> None:
    """El fallback debe emitir ``datafeed_connected`` cuando el WS esté activo."""

    class _Feed:
        def __init__(self) -> None:
            self._managed_by_trader = True
            self.ws_connected_event = asyncio.Event()
            self.ws_failed_event = asyncio.Event()

        async def start(self) -> None:
            async def _mark_connected() -> None:
                await asyncio.sleep(0.01)
                self.ws_connected_event.set()

            asyncio.create_task(_mark_connected())

    class _Bus:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict[str, Any]]] = []

        def emit(self, name: str, payload: dict[str, Any]) -> None:
            self.events.append((name, payload))

    class _Trader:
        def __init__(self, feed: _Feed, bus: _Bus) -> None:
            self.data_feed = feed
            self.event_bus = bus
            self.started = asyncio.Event()

        async def ejecutar(self) -> None:
            self.started.set()
            await asyncio.sleep(5)

    feed = _Feed()
    bus = _Bus()
    trader = _Trader(feed, bus)
    manager = StartupManager(
        trader=trader,
        config={"symbols": ["btcusdt"], "intervalo_velas": "1m"},
    )

    await manager._open_streams()
    await asyncio.wait_for(trader.started.wait(), timeout=0.5)
    await asyncio.wait_for(feed.ws_connected_event.wait(), timeout=0.5)

    assert manager._fallback_ws_signal_task is not None
    await asyncio.wait_for(manager._fallback_ws_signal_task, timeout=1.0)

    assert ("datafeed_connected", {"symbols": ["BTCUSDT"]}) in bus.events

    await manager._stop_streams()


@pytest.mark.asyncio
async def test_open_streams_fallback_logs_running_without_bus() -> None:
    """Si no hay bus de eventos, debe registrarse ``trader:ws_started``."""

    class _Feed:
        def __init__(self) -> None:
            self._managed_by_trader = True
            self.ws_connected_event = asyncio.Event()
            self.ws_failed_event = asyncio.Event()

        async def start(self) -> None:
            self.ws_connected_event.set()

    class _Trader:
        def __init__(self, feed: _Feed) -> None:
            self.data_feed = feed
            self.started = asyncio.Event()

        async def ejecutar(self) -> None:
            self.started.set()
            await asyncio.sleep(5)

    feed = _Feed()
    trader = _Trader(feed)
    manager = StartupManager(
        trader=trader,
        config={"symbols": ["ethusdt"], "intervalo_velas": "1m"},
    )
    manager.log = _CapturingLogger()

    await manager._open_streams()
    await asyncio.wait_for(trader.started.wait(), timeout=0.5)
    await asyncio.wait_for(feed.ws_connected_event.wait(), timeout=0.5)

    assert manager._fallback_ws_signal_task is not None
    await asyncio.wait_for(manager._fallback_ws_signal_task, timeout=1.0)

    assert any(call["message"] == "trader:ws_started" for call in manager.log.info_calls)
    info_extra = [call["kwargs"].get("extra", {}) for call in manager.log.info_calls if call["message"] == "trader:ws_started"]
    assert any(extra.get("reason") == "fallback_autostart" for extra in info_extra)

    await manager._stop_streams()
