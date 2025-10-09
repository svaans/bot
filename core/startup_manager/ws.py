"""Herramientas de monitoreo del WebSocket durante el arranque."""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from contextlib import suppress
from typing import Any, Optional


class WebSocketMonitorMixin:
    """Funcionalidad de espera y supervisión del WebSocket."""

    trader: Any
    config: Any
    log: Any
    ws_timeout: Optional[float]
    event_bus: Any | None
    _ws_started_logged: bool

    def _resolve_ws_timeout(self) -> float:
        assert self.trader is not None and self.config is not None

        raw_timeout = getattr(self.config, "ws_timeout", None) or self.ws_timeout
        if raw_timeout is None:
            raw_timeout = float(os.getenv("WS_TIMEOUT", "10"))
            with suppress(Exception):
                setattr(self.config, "ws_timeout", raw_timeout)
            trader_cfg = getattr(self.trader, "config", None)
            if trader_cfg is not None and trader_cfg is not self.config:
                with suppress(Exception):
                    setattr(trader_cfg, "ws_timeout", raw_timeout)

        try:
            timeout_val = float(raw_timeout)
        except (TypeError, ValueError):
            timeout_val = float(os.getenv("WS_TIMEOUT", "30"))

        self.ws_timeout = timeout_val
        return timeout_val

    async def _wait_ws(self, timeout: float, *, grace_for_trader: float = 5.0) -> None:
        assert self.trader is not None

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        managed = bool(getattr(feed, "_managed_by_trader", False)) if feed is not None else False
        managed_override = self._get_config_value("ws_managed_by_trader", None)
        if managed_override is not None:
            managed = bool(managed_override)

        grace = max(0.0, float(grace_for_trader))
        if grace > timeout:
            grace = float(timeout)
        self.log.debug(
            "wait_ws:begin",
            extra={
                "timeout": timeout,
                "grace_for_trader": grace,
                "managed_by_trader": managed,
            },
        )

        if await self._poll_ws_connected():
            self.log.debug(
                "wait_ws:already_connected",
                extra={"timeout": timeout},
            )
            return

        trader_connected = False
        trader_activity = False

        if managed and grace > 0.0:
            trader_connected, trader_activity = await self._wait_for_trader_grace(grace)
            if trader_connected:
                self.log.debug(
                    "wait_ws:trader_ready",
                    extra={"grace_for_trader": grace},
                )
                return
            if not trader_activity:
                self.log.warning("Trader no inició WS en la gracia; forzando ensure_running()")
                self._set_config_value("ws_managed_by_trader", False)
                if feed is not None:
                    with suppress(Exception):
                        setattr(feed, "_managed_by_trader", False)
                managed = False

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)

        if feed is not None and hasattr(feed, "ensure_running"):
            if not trader_activity and not await self._is_ws_attempt_in_progress():
                self.log.debug(
                    "wait_ws:ensure_running_fallback",
                    extra={"grace_for_trader": grace},
                )
                ensure_running = getattr(feed, "ensure_running")
                try:
                    result = ensure_running()
                except TypeError:
                    result = None
                if inspect.isawaitable(result):
                    await result
                else:
                    await asyncio.sleep(0)

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)

        def _log_waiting(event_bus_flag: bool, asyncio_flag: bool, metric_flag: bool, polling_flag: bool) -> None:
            self.log.info(
                "waiting on: event_bus=%s, asyncio_event=%s, metric=%s, polling=%s",
                event_bus_flag,
                asyncio_flag,
                metric_flag,
                polling_flag,
            )

        def _log_missing(event_bus_flag: bool, asyncio_flag: bool, metric_flag: bool, polling_flag: bool) -> None:
            self.log.warning(
                "no se detectó señal event_bus=%s, asyncio_event=%s, metric=%s, polling=%s",
                event_bus_flag,
                asyncio_flag,
                metric_flag,
                polling_flag,
            )

        metric_available = bool(getattr(self, "metrics", None))
        polling_active = False

        if managed:
            bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            wait_fn = getattr(bus, "wait", None) if bus is not None else None
            if callable(wait_fn):
                _log_waiting(True, False, metric_available, False)
                try:
                    await asyncio.wait_for(wait_fn("datafeed_connected"), timeout=timeout)
                    return
                except asyncio.TimeoutError as exc:
                    _log_missing(True, False, metric_available, False)
                    raise RuntimeError("WS no conectado") from exc
            else:
                self.log.debug(
                    "EventBus sin soporte de wait() o ausente; se recurre a sondeo de actividad.",
                )

        success_event = getattr(feed, "ws_connected_event", None)
        failure_event = getattr(feed, "ws_failed_event", None)

        def _event_is_set(evt: Any) -> bool:
            if evt is None:
                return False
            if isinstance(evt, asyncio.Event):
                return evt.is_set()
            is_set = getattr(evt, "is_set", None)
            if callable(is_set):
                try:
                    return bool(is_set())
                except Exception:
                    return False
            return False

        def _failure_message() -> str:
            reason = getattr(feed, "ws_failure_reason", None)
            base = "WS no conectado"
            if reason:
                return f"{base}: {reason}"
            return base

        if _event_is_set(failure_event):
            raise RuntimeError(_failure_message())
        if _event_is_set(success_event):
            return

        async_events: dict[str, asyncio.Event] = {}
        thread_events: dict[str, Any] = {}
        if isinstance(success_event, asyncio.Event):
            async_events["success"] = success_event
        elif success_event is not None and callable(getattr(success_event, "wait", None)):
            thread_events["success"] = success_event
        if isinstance(failure_event, asyncio.Event):
            async_events["failure"] = failure_event
        elif failure_event is not None and callable(getattr(failure_event, "wait", None)):
            thread_events["failure"] = failure_event

        asyncio_available = bool(async_events or thread_events)
        polling_active = not asyncio_available
        _log_waiting(False, asyncio_available, metric_available, polling_active)

        async def _wait_async_evt(evt: asyncio.Event) -> bool:
            try:
                await asyncio.wait_for(evt.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                return False
            return True

        async def _wait_thread_evt(evt: Any) -> bool | None:
            loop = asyncio.get_running_loop()
            wait_fn = getattr(evt, "wait", None)
            if not callable(wait_fn):
                return None
            try:
                result = await loop.run_in_executor(None, lambda: wait_fn(timeout))
            except Exception:
                self.log.debug("Fallo al esperar threading.Event; degradando a sondeo.", exc_info=True)
                return None
            return bool(result)

        wait_tasks: dict[str, asyncio.Task[bool | None]] = {}
        for label, evt in async_events.items():
            wait_tasks[label] = asyncio.create_task(_wait_async_evt(evt), name=f"wait_ws_{label}")
        for label, evt in thread_events.items():
            wait_tasks[label] = asyncio.create_task(_wait_thread_evt(evt), name=f"wait_ws_{label}")

        if wait_tasks:
            try:
                done, _ = await asyncio.wait(
                    wait_tasks.values(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
            except Exception:
                self.log.debug("Fallo al esperar ws_connected_event; se recurre a sondeo.", exc_info=True)
            else:
                if not done:
                    _log_missing(False, asyncio_available, metric_available, polling_active)
                    raise RuntimeError(_failure_message())
                degrade_to_poll = False
                for label, task in wait_tasks.items():
                    if task in done:
                        try:
                            triggered = task.result()
                        except Exception:
                            triggered = None
                        if triggered is None or triggered is False:
                            degrade_to_poll = True
                            continue
                        if label == "success" and triggered:
                            return
                        if label == "failure" and triggered:
                            raise RuntimeError(_failure_message())
                if degrade_to_poll:
                    self.log.debug("Eventos incompatibles con espera directa; degradando a sondeo.")
                    asyncio_available = False
                    polling_active = True
                    _log_waiting(False, False, metric_available, polling_active)
                else:
                    _log_missing(False, asyncio_available, metric_available, polling_active)
                    raise RuntimeError(_failure_message())
            finally:
                for task in wait_tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*wait_tasks.values(), return_exceptions=True)

        start = time.time()
        while time.time() - start < timeout:
            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is None:
                await asyncio.sleep(0.1)
                continue

            if await self._poll_ws_connected():
                return

            activo = False
            success_event = getattr(feed, "ws_connected_event", success_event)
            failure_event = getattr(feed, "ws_failed_event", failure_event)
            if _event_is_set(failure_event):
                raise RuntimeError(_failure_message())
            if _event_is_set(success_event):
                return
            if hasattr(feed, "activos"):
                try:
                    val = feed.activos
                    activo = bool(val)
                except TypeError:
                    with suppress(Exception):
                        activo = bool(feed.activos())
                except Exception:
                    activo = False
            if not activo and hasattr(feed, "is_active"):
                with suppress(Exception):
                    activo = bool(feed.is_active())

            if activo:
                return
            await asyncio.sleep(0.1)
        _log_missing(False, asyncio_available, metric_available, True)
        raise RuntimeError(_failure_message())

    async def _wait_for_trader_grace(self, timeout: float) -> tuple[bool, bool]:
        if timeout <= 0:
            connected = await self._poll_ws_connected()
            if connected:
                return True, True
            return False, await self._is_ws_attempt_in_progress()

        bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
        wait_fn = getattr(bus, "wait", None) if bus is not None else None

        tasks: dict[str, asyncio.Task[Any]] = {}
        if callable(wait_fn):
            for event_name in ("datafeed_connected", "ws_connected", "ws_connecting"):
                try:
                    result = wait_fn(event_name)
                except Exception:
                    continue
                if not inspect.isawaitable(result):
                    continue
                tasks[event_name] = asyncio.create_task(result, name=f"wait_ws_grace_{event_name}")

        attempt_detected = False
        deadline = time.monotonic() + timeout
        try:
            while True:
                for name, task in list(tasks.items()):
                    if not task.done():
                        continue
                    try:
                        _ = task.result()
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        tasks.pop(name, None)
                        continue
                    attempt_detected = True
                    if name in {"datafeed_connected", "ws_connected"}:
                        return True, True
                    tasks.pop(name, None)

                if await self._poll_ws_connected():
                    return True, True

                if await self._is_ws_attempt_in_progress():
                    attempt_detected = True

                now = time.monotonic()
                remaining = deadline - now
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.1, remaining))
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)

        return False, attempt_detected

    async def _is_ws_attempt_in_progress(self) -> bool:
        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        if feed is None:
            return False

        failure_event = getattr(feed, "ws_failed_event", None)
        if isinstance(failure_event, asyncio.Event) and failure_event.is_set():
            return False
        maybe_is_set = getattr(failure_event, "is_set", None)
        if callable(maybe_is_set):
            with suppress(Exception):
                if bool(maybe_is_set()):
                    return False

        if await self._poll_ws_connected():
            return True

        if bool(getattr(feed, "_running", False)):
            return True

        activos = getattr(feed, "activos", None)
        try:
            if isinstance(activos, bool):
                if activos:
                    return True
            elif callable(activos):
                result = activos()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
        except Exception:
            pass

        is_active = getattr(feed, "is_active", None)
        if callable(is_active):
            try:
                result = is_active()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
            except Exception:
                pass

        tasks = getattr(feed, "_tasks", None)
        if isinstance(tasks, dict):
            for task in tasks.values():
                if isinstance(task, asyncio.Task) and not task.done():
                    return True

        connecting_event = getattr(feed, "ws_connecting_event", None)
        if isinstance(connecting_event, asyncio.Event) and connecting_event.is_set():
            return True
        maybe_is_set = getattr(connecting_event, "is_set", None)
        if callable(maybe_is_set):
            with suppress(Exception):
                if bool(maybe_is_set()):
                    return True

        return False

    async def _poll_ws_connected(self) -> bool:
        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        if feed is None:
            return False

        connected_attr = getattr(feed, "connected", None)
        try:
            if isinstance(connected_attr, bool):
                if connected_attr:
                    return True
            elif callable(connected_attr):
                result = connected_attr()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
        except Exception:
            pass

        evt = getattr(feed, "ws_connected_event", None)
        if isinstance(evt, asyncio.Event):
            if evt.is_set():
                return True
        else:
            maybe_is_set = getattr(evt, "is_set", None)
            if callable(maybe_is_set):
                with suppress(Exception):
                    if bool(maybe_is_set()):
                        return True

        metrics = getattr(self, "metrics", None)
        gauge = getattr(metrics, "ws_connected_gauge", None) if metrics is not None else None
        if gauge is not None:
            getter = getattr(gauge, "get", None)
            if callable(getter):
                with suppress(Exception):
                    return bool(getter())

        return False

    def _set_config_value(self, key: str, value: Any) -> None:
        """Guarda ``key`` en la configuración activa si está disponible."""

        cfg = getattr(self, "config", None)
        if cfg is None:
            trader = getattr(self, "trader", None)
            cfg = getattr(trader, "config", None) if trader is not None else None

        if cfg is None:
            return

        if hasattr(cfg, "__setitem__"):
            try:
                cfg[key] = value  # type: ignore[index]
                return
            except Exception:
                pass

        with suppress(Exception):
            setattr(cfg, key, value)

    def _get_config_value(self, key: str, default: Any | None = None) -> Any | None:
        """Obtiene un valor de la configuración respetando un ``default`` seguro."""

        cfg = getattr(self, "config", None)
        if cfg is None:
            trader = getattr(self, "trader", None)
            cfg = getattr(trader, "config", None) if trader is not None else None

        if cfg is None:
            return default

        getter = getattr(cfg, "get", None)
        if callable(getter):
            try:
                return getter(key, default)
            except Exception:
                return default

        return getattr(cfg, key, default)
