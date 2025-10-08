"""Gestión de feeds y tareas auxiliares durante el arranque."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import suppress
from typing import Any, Optional


class FeedLifecycleMixin:
    """Funciones relacionadas con el ciclo de vida del DataFeed."""

    trader: Any
    config: Any
    log: Any
    task: Optional[asyncio.Task]
    event_bus: Any | None
    _trader_hold: Optional[asyncio.Event]
    _feed_task: Optional[asyncio.Task]
    _fallback_ws_signal_task: Optional[asyncio.Task]
    _ws_started_logged: bool
    _datafeed_connected_announced: bool

    async def _open_streams(self) -> None:
        assert self.trader is not None

        start_fn = getattr(self.trader, "ejecutar", None) or getattr(self.trader, "run", None)
        if start_fn is None:
            raise AttributeError("Trader no expone métodos ejecutar() ni run()")

        self._trader_hold = asyncio.Event()

        async def _run_trader() -> None:
            exc: BaseException | None = None
            closing = False
            try:
                result = start_fn()
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                closing = True
                raise
            except GeneratorExit:
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                closing = True
                raise
            except BaseException as err:  # pragma: no cover
                exc = err
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                self.log.error("Trader finalizó con error inesperado: %s", err)
            finally:
                if getattr(self, "_trader_hold", None) is not None:
                    with suppress(Exception):
                        self._trader_hold.set()
                    wait_fn = getattr(self._trader_hold, "wait", None)
                    if not closing and callable(wait_fn):
                        try:
                            res = wait_fn()
                            if inspect.isawaitable(res):
                                with suppress(Exception):
                                    await asyncio.wait_for(res, timeout=1.0)
                        except Exception:
                            pass
                if exc is not None:
                    raise exc

        self.task = asyncio.create_task(_run_trader(), name="TraderMain")

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        manual_feed = bool(getattr(feed, "_managed_by_trader", False)) if feed is not None else False
        start_feed = getattr(feed, "start", None) or getattr(feed, "iniciar", None) if feed else None

        async def _launch_feed(reason: str) -> None:
            if start_feed is None:
                return
            try:
                result = start_feed()
            except TypeError:
                result = None
            if inspect.isawaitable(result):
                self._feed_task = asyncio.create_task(result, name="DataFeedStart")
                await asyncio.sleep(0)
            self.log.debug("DataFeed iniciado (%s).", reason)

        bus = None
        if self.trader is not None:
            bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            if bus is not None:
                self.event_bus = bus

        wait_fn = getattr(bus, "wait", None) if bus is not None else None
        schedule_ws_signal = False

        if start_feed is not None and not manual_feed:
            await _launch_feed("startup_manager")
        elif manual_feed:
            if callable(wait_fn):
                self.log.debug("DataFeed gestionado por Trader; se omite arranque automático.")
            else:
                self.log.warning(
                    "DataFeed gestionado pero sin soporte de EventBus.wait(); se arranca como fallback.",
                )
                if feed is not None:
                    symbols_cfg = getattr(self.config, "symbols", None)
                    symbols_list: list[Any] = []
                    if symbols_cfg is not None:
                        try:
                            symbols_list = [str(symbol).upper() for symbol in symbols_cfg]
                        except Exception:
                            try:
                                symbols_list = list(symbols_cfg)  # type: ignore[arg-type]
                            except Exception:
                                symbols_list = []
                    feed._symbols = symbols_list  # type: ignore[attr-defined]
                    feed._handler = getattr(self.trader, "_handler", None)  # type: ignore[attr-defined]
                    if hasattr(self.trader, "_cliente"):
                        feed._cliente = getattr(self.trader, "_cliente")  # type: ignore[attr-defined]
                self._set_config_value("ws_managed_by_trader", False)
                if feed is not None:
                    with suppress(Exception):
                        setattr(feed, "_managed_by_trader", False)
                await _launch_feed("fallback_autostart")
                schedule_ws_signal = True

        if schedule_ws_signal and feed is not None:
            self._schedule_fallback_ws_signal(feed)

    def _schedule_fallback_ws_signal(self, feed: Any, *, timeout: float = 5.0) -> None:
        task = self._fallback_ws_signal_task
        if task is not None and not task.done():
            task.cancel()

        async def _runner() -> None:
            try:
                await self._await_fallback_ws_connected(feed, timeout=max(0.1, float(timeout)))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.debug(
                    "Error al esperar confirmación de WS tras fallback_autostart.",
                    exc_info=True,
                )

        self._fallback_ws_signal_task = asyncio.create_task(
            _runner(), name="StartupFallbackWsSignal"
        )

    async def _await_fallback_ws_connected(self, feed: Any, *, timeout: float) -> None:
        if self._datafeed_connected_announced:
            return

        connected_event = getattr(feed, "ws_connected_event", None)
        failure_event = getattr(feed, "ws_failed_event", None)

        def _event_is_set(evt: Any) -> bool:
            if evt is None:
                return False
            is_set = getattr(evt, "is_set", None)
            if callable(is_set):
                with suppress(Exception):
                    return bool(is_set())
            return False

        if _event_is_set(connected_event):
            await self._finalize_fallback_ws_connected(feed)
            return

        wait_tasks: dict[str, asyncio.Task[Any]] = {}

        def _prepare_wait(evt: Any, label: str) -> None:
            if evt is None:
                return
            wait_fn = getattr(evt, "wait", None)
            if not callable(wait_fn):
                return
            try:
                result = wait_fn()
            except Exception:
                return
            if not inspect.isawaitable(result):
                return
            wait_tasks[label] = asyncio.create_task(result, name=f"fallback_ws_{label}")

        _prepare_wait(connected_event, "connected")
        _prepare_wait(failure_event, "failed")

        if wait_tasks:
            done: set[asyncio.Task[Any]] = set()
            pending: set[asyncio.Task[Any]] = set()
            try:
                done, pending = await asyncio.wait(
                    wait_tasks.values(),
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in pending:
                    task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await task
                for task in done:
                    with suppress(Exception):
                        task.result()

            if not done:
                self.log.warning(
                    "No se confirmó la conexión del DataFeed tras fallback_autostart (timeout).",
                )
                return
            if wait_tasks.get("failed") in done:
                self.log.warning(
                    "WS no conectado tras fallback_autostart; feed reportó ws_failed_event.",
                    extra={"reason": getattr(feed, "ws_failure_reason", None)},
                )
                return
            if wait_tasks.get("connected") not in done:
                return

            await self._finalize_fallback_ws_connected(feed)
            return

        if await self._coerce_feed_connected(feed):
            await self._finalize_fallback_ws_connected(feed)

    async def _coerce_feed_connected(self, feed: Any) -> bool:
        connected_attr = getattr(feed, "connected", None)
        try:
            if isinstance(connected_attr, bool):
                return connected_attr
            if callable(connected_attr):
                result = connected_attr()
                if inspect.isawaitable(result):
                    result = await result
                return bool(result)
        except Exception:
            self.log.debug("Fallo al consultar feed.connected durante fallback.", exc_info=True)
        return False

    async def _finalize_fallback_ws_connected(self, feed: Any) -> None:
        if self._datafeed_connected_announced:
            return

        signal_fn = getattr(feed, "_signal_ws_connected", None)
        if callable(signal_fn):
            with suppress(Exception):
                signal_fn(None)

        symbols_cfg = self._get_config_value("symbols", None)
        symbols: list[str] = []
        if symbols_cfg:
            try:
                symbols_iter = list(symbols_cfg)
            except Exception:
                symbols_iter = []
            else:
                try:
                    symbols = [str(symbol).upper() for symbol in symbols_iter]
                except Exception:
                    symbols = []

        bus_payload: dict[str, Any] = {}
        if symbols:
            bus_payload["symbols"] = symbols

        intervalo = self._get_config_value("intervalo_velas", None)
        log_extra: dict[str, Any] = {"stage": "StartupManager"}
        if intervalo is not None:
            log_extra["intervalo"] = intervalo
        if symbols:
            log_extra["symbols"] = symbols
        log_extra["reason"] = "fallback_autostart"

        if not self._ws_started_logged:
            self.log.info("trader:ws_started", extra=log_extra)
            self._ws_started_logged = True

        bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
        announced = False
        if bus is not None:
            emit = getattr(bus, "emit", None)
            if callable(emit):
                try:
                    emit("datafeed_connected", dict(bus_payload))
                    announced = True
                except Exception:
                    self.log.debug("No se pudo emitir datafeed_connected en event_bus.", exc_info=True)
            if not announced:
                publish = getattr(bus, "publish", None)
                if callable(publish):
                    try:
                        asyncio.create_task(publish("datafeed_connected", dict(bus_payload)))
                        announced = True
                    except Exception:
                        self.log.debug(
                            "No se pudo publicar datafeed_connected en event_bus.",
                            exc_info=True,
                        )

        if announced or bus is None:
            self._datafeed_connected_announced = True

    async def _stop_streams(self) -> None:
        if getattr(self, "_feed_task", None) is not None:
            self._feed_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._feed_task
            self._feed_task = None
        if getattr(self, "_fallback_ws_signal_task", None) is not None:
            self._fallback_ws_signal_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._fallback_ws_signal_task
            self._fallback_ws_signal_task = None
        if getattr(self, "_trader_hold", None) is not None:
            self._trader_hold.set()
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self.task
        self.task = None

    def _set_config_value(self, key: str, value: Any) -> None:
        raise NotImplementedError

    def _get_config_value(self, key: str, default: Any | None = None) -> Any | None:
        raise NotImplementedError
