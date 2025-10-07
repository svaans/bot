from __future__ import annotations

"""Implementación principal del ``DataFeed`` modularizado."""

import asyncio
import contextlib
import os
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from core.utils.utils import intervalo_a_segundos

from . import backfill as backfill_module
from . import events as events_module
from . import handlers as handlers_module
from . import monitors as monitors_module
from . import streaming as streaming_module
from ._shared import COMBINED_STREAM_KEY, ConsumerState, _safe_float, _safe_int, log


class DataFeed:
    """DataFeed robusto y legible para velas cerradas."""

    def __init__(
        self,
        intervalo: str,
        *,
        handler_timeout: float = float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
        inactivity_intervals: int = int(os.getenv("DF_INACTIVITY_INTERVALS", "10")),
        queue_max: int = int(os.getenv("DF_QUEUE_MAX", "2000")),
        queue_policy: str = os.getenv("DF_QUEUE_POLICY", "drop_oldest").lower(),
        monitor_interval: float = float(os.getenv("DF_MONITOR_INTERVAL", "5")),
        consumer_timeout_intervals: float | None = None,
        consumer_timeout_min: float | None = None,
        backpressure: bool = os.getenv("DF_BACKPRESSURE", "true").lower() == "true",
        cancel_timeout: float = float(os.getenv("DF_CANCEL_TIMEOUT", "5")),
        on_event: Optional[Callable[[str, dict], None]] = None,
        event_bus: Any | None = None,
        max_reconnect_attempts: int | None = None,
        max_reconnect_time: float | None = None,
    ) -> None:
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)

        self.handler_timeout = max(0.1, float(handler_timeout))
        self.inactivity_intervals = max(1, int(inactivity_intervals))
        self.tiempo_inactividad = self.intervalo_segundos * self.inactivity_intervals

        self.queue_max = max(0, int(queue_max))
        self.queue_policy = queue_policy if queue_policy in {"drop_oldest", "block"} else "drop_oldest"

        self.monitor_interval = max(0.05, float(monitor_interval))

        if consumer_timeout_intervals is None:
            consumer_timeout_intervals = _safe_float(os.getenv("DF_CONSUMER_TIMEOUT_INTERVALS", "1.5"), 1.5)
        self.consumer_timeout_intervals = max(1.0, float(consumer_timeout_intervals))

        if consumer_timeout_min is None:
            consumer_timeout_min = _safe_float(os.getenv("DF_CONSUMER_TIMEOUT_MIN", "30.0"), 30.0)
        self.consumer_timeout_min = max(0.0, float(consumer_timeout_min))

        raw_consumer_timeout = self.intervalo_segundos * self.consumer_timeout_intervals
        self.consumer_timeout = max(self.consumer_timeout_min, min(self.tiempo_inactividad, raw_consumer_timeout))

        self.backpressure = bool(backpressure)
        self.cancel_timeout = max(0.5, float(cancel_timeout))
        self.on_event = on_event
        self.event_bus = event_bus
        self._set_ws_connection_metric(0.0)

        if max_reconnect_attempts is None:
            env_max_retries = _safe_int(os.getenv("DF_WS_MAX_RETRIES", "0"), 0)
            max_reconnect_attempts = env_max_retries
        if max_reconnect_attempts is not None and max_reconnect_attempts <= 0:
            max_reconnect_attempts = None
        self.max_reconnect_attempts: int | None = (
            int(max_reconnect_attempts) if max_reconnect_attempts is not None else None
        )

        if max_reconnect_time is None:
            env_max_downtime = _safe_float(os.getenv("DF_WS_MAX_DOWNTIME_SEC", "0"), 0.0)
            max_reconnect_time = env_max_downtime
        if max_reconnect_time is not None and max_reconnect_time <= 0:
            max_reconnect_time = None
        self.max_reconnect_time: float | None = (
            float(max_reconnect_time) if max_reconnect_time is not None else None
        )

        self._running = False
        self._symbols: List[str] = []
        self._cliente: Any | None = None
        self._handler: Callable[[dict], Awaitable[None]] | None = None
        self._combined = False

        self._queues: Dict[str, asyncio.Queue] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        self._last_close_ts: Dict[str, int] = {}
        self._ultimo_candle: Dict[str, dict] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._consumer_last: Dict[str, float] = {}
        self._consumer_state: Dict[str, ConsumerState] = {}
        self._reconnect_attempts: Dict[str, int] = {}
        self._reconnect_since: Dict[str, float] = {}

        self._monitor_inactividad_task: asyncio.Task | None = None
        self._monitor_consumers_task: asyncio.Task | None = None

        self.min_buffer_candles = int(os.getenv("MIN_BUFFER_CANDLES", "30"))
        self._last_backfill_ts: Dict[str, int] = {}
        self._backfill_max = int(os.getenv("BACKFILL_MAX_CANDLES", "1000"))

        self._managed_by_trader: bool = bool(getattr(self, "_managed_by_trader", False))

        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._handler_expected_info: dict[str, int | str | None] | None = None
        self._handler_wrapper_calls: int = 0
        self._handler_log_events: int = 0

        self.ws_connected_event: asyncio.Event = asyncio.Event()
        self.ws_failed_event: asyncio.Event = asyncio.Event()
        self._ws_failure_reason: str | None = None

    @property
    def activos(self) -> bool:
        if not self._running:
            return False
        return any(not task.done() for task in self._tasks.values())

    def is_active(self) -> bool:
        return self.activos

    def verificar_continuidad(self, *, max_gap_intervals: int | None = None) -> bool:
        if not self._symbols:
            return True
        intervalo_ms = self.intervalo_segundos * 1000
        tolerancia = (max_gap_intervals or 1) * intervalo_ms
        for symbol in self._symbols:
            ultimo = self._last_close_ts.get(symbol)
            if ultimo is None:
                return False
            backfill = self._last_backfill_ts.get(symbol)
            if backfill is None:
                continue
            if ultimo - backfill > tolerancia:
                return False
        return True

    async def precargar(
        self,
        symbols: Iterable[str],
        *,
        cliente: Any | None = None,
        minimo: int | None = None,
    ) -> None:
        symbols_list = [s.upper() for s in symbols]
        if not symbols_list:
            return

        prev_cliente = self._cliente
        prev_symbols = list(self._symbols)
        prev_min = self.min_buffer_candles

        if cliente is not None:
            self._cliente = cliente
        if minimo is not None:
            try:
                self.min_buffer_candles = max(1, int(minimo))
            except (TypeError, ValueError):
                pass

        try:
            self._symbols = symbols_list
            for symbol in symbols_list:
                await self._do_backfill(symbol)
        finally:
            if minimo is not None:
                self.min_buffer_candles = prev_min
            self._symbols = prev_symbols
            if cliente is not None:
                self._cliente = prev_cliente

    async def escuchar(
        self,
        symbols: Iterable[str],
        handler: Callable[[dict], Awaitable[None]],
        *,
        cliente: Any | None = None,
    ) -> None:
        await self.detener()

        if isinstance(self.ws_connected_event, asyncio.Event):
            self.ws_connected_event.clear()
        else:
            self.ws_connected_event = asyncio.Event()
        if isinstance(self.ws_failed_event, asyncio.Event):
            self.ws_failed_event.clear()
        else:
            self.ws_failed_event = asyncio.Event()
        self._ws_failure_reason = None

        self._symbols = list({s.upper() for s in symbols})
        if not self._symbols:
            log.warning("Sin símbolos para escuchar()")
            return

        self._handler_log_events = 0
        self._handler_wrapper_calls = 0
        handler_info = self._describe_handler(handler)
        self._handler_expected_info = handler_info
        log.debug(
            "handler.registered",
            extra={
                "stage": "DataFeed.escuchar",
                **{k: v for k, v in handler_info.items() if v is not None},
            },
        )

        self._handler = handler
        self._cliente = cliente
        self._combined = len(self._symbols) > 1
        self._running = True
        self._reconnect_attempts.clear()
        self._reconnect_since.clear()

        log.info(
            "suscrito",
            extra={"symbols": self._symbols, "intervalo": self.intervalo, "combined": self._combined},
        )

        self._queues = {s: asyncio.Queue(maxsize=self.queue_max) for s in self._symbols}
        for symbol in self._symbols:
            self._set_consumer_state(symbol, ConsumerState.STARTING)
            self._consumer_tasks[symbol] = asyncio.create_task(
                self._consumer(symbol), name=f"consumer_{symbol}"
            )

        self._monitor_inactividad_task = asyncio.create_task(
            self._monitor_inactividad(), name="monitor_inactividad"
        )
        self._monitor_consumers_task = asyncio.create_task(
            self._monitor_consumers(self.consumer_timeout), name="monitor_consumers"
        )

        if self._combined:
            task = asyncio.create_task(self._stream_combinado(self._symbols), name="stream_combinado")
            for symbol in self._symbols:
                self._tasks[symbol] = task
        else:
            for symbol in self._symbols:
                self._tasks[symbol] = asyncio.create_task(
                    self._stream_simple(symbol), name=f"stream_{symbol}"
                )

        while self._running and any(not task.done() for task in self._tasks.values()):
            await asyncio.sleep(0.1)

    async def iniciar(self) -> None:
        if not self._symbols or not self._handler:
            log.warning("No se puede iniciar(): faltan símbolos o handler")
            return
        await self.escuchar(self._symbols, self._handler, cliente=self._cliente)

    async def start(self) -> None:
        await self.iniciar()

    async def detener(self) -> None:
        was_running = self._running
        was_connected_signal = False
        evt = getattr(self, "ws_connected_event", None)
        if isinstance(evt, asyncio.Event):
            was_connected_signal = evt.is_set()
        else:
            maybe_is_set = getattr(evt, "is_set", None)
            if callable(maybe_is_set):
                with contextlib.suppress(Exception):
                    was_connected_signal = bool(maybe_is_set())
        tasks: set[asyncio.Task] = set()
        tasks.update(self._tasks.values())
        tasks.update(self._consumer_tasks.values())
        if self._monitor_inactividad_task:
            tasks.add(self._monitor_inactividad_task)
        if self._monitor_consumers_task:
            tasks.add(self._monitor_consumers_task)

        for task in tasks:
            task.cancel()

        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.cancel_timeout)

            for task in done:
                try:
                    _ = task.result()
                except asyncio.CancelledError:
                    continue
                except Exception as exc:
                    log.error(
                        "tarea_finalizo_con_error",
                        extra={"task_name": task.get_name(), "error": repr(exc)},
                    )

            if pending:
                log.error(
                    "tareas_no_finalizaron_a_tiempo",
                    extra={
                        "timeout": self.cancel_timeout,
                        "pending_tasks": [task.get_name() for task in pending],
                    },
                )
                for task in pending:
                    task.cancel()

        self._running = False
        self._tasks.clear()
        self._consumer_tasks.clear()
        self._queues.clear()
        self._monitor_inactividad_task = None
        self._monitor_consumers_task = None
        self._consumer_last.clear()
        self._consumer_state.clear()
        self._reconnect_attempts.clear()
        self._reconnect_since.clear()

        if isinstance(self.ws_connected_event, asyncio.Event):
            self.ws_connected_event.clear()
        else:
            self.ws_connected_event = asyncio.Event()
        if isinstance(self.ws_failed_event, asyncio.Event):
            self.ws_failed_event.clear()
        else:
            self.ws_failed_event = asyncio.Event()
        self._ws_failure_reason = None
        if was_running or was_connected_signal:
            self._mark_ws_state(False, {"reason": "detener"})

    async def _stream_simple(self, symbol: str) -> None:
        await streaming_module.stream_simple(self, symbol)

    async def _stream_combinado(self, symbols: List[str]) -> None:
        await streaming_module.stream_combinado(self, symbols)

    async def _handle_candle(self, symbol: str, candle: dict) -> None:
        await handlers_module.handle_candle(self, symbol, candle)

    async def _consumer(self, symbol: str) -> None:
        await handlers_module.consumer_loop(self, symbol)

    async def _monitor_inactividad(self) -> None:
        await monitors_module.monitor_inactividad(self)

    async def _monitor_consumers(self, timeout: float | None = None) -> None:
        await monitors_module.monitor_consumers(self, timeout)

    async def _reiniciar_stream(self, symbol: str) -> None:
        await monitors_module.reiniciar_stream(self, symbol)

    async def _reiniciar_consumer(self, symbol: str) -> None:
        await monitors_module.reiniciar_consumer(self, symbol)

    async def _do_backfill(self, symbol: str) -> None:
        await backfill_module.do_backfill(self, symbol)

    def _note_handler_log(self) -> None:
        handlers_module.note_handler_log(self)

    def _describe_handler(self, handler: Callable[[dict], Awaitable[None]]) -> dict[str, int | str | None]:
        return handlers_module.describe_handler(handler)

    def _activate_handler_debug_wrapper(self) -> None:
        handlers_module.activate_handler_debug_wrapper(self)

    def _set_consumer_state(self, symbol: str, state: ConsumerState) -> None:
        events_module.set_consumer_state(self, symbol, state)

    def _signal_ws_connected(self, symbol: str | None = None) -> None:
        events_module.signal_ws_connected(self, symbol)

    def _signal_ws_failure(self, reason: Any) -> None:
        events_module.signal_ws_failure(self, reason)

    def _register_reconnect_attempt(self, key: str, reason: str) -> bool:
        return events_module.register_reconnect_attempt(self, key, reason)

    def _reset_reconnect_tracking(self, key: str) -> None:
        events_module.reset_reconnect_tracking(self, key)

    def _set_ws_connection_metric(self, value: float) -> None:
        events_module.set_ws_connection_metric(value)

    def _emit_bus_signal(self, event: str, payload: dict[str, Any]) -> None:
        events_module.emit_bus_signal(self, event, payload)

    def _mark_ws_state(self, connected: bool, payload: dict[str, Any] | None = None) -> None:
        events_module.mark_ws_state(self, connected, payload)

    def _emit(self, evento: str, data: dict) -> None:
        events_module.emit_event(self, evento, data)

    @property
    def ws_failure_reason(self) -> str | None:
        return self._ws_failure_reason

    def stats(self) -> Dict[str, Dict[str, int]]:
        return {symbol: dict(values) for symbol, values in self._stats.items()}


__all__ = ["DataFeed", "ConsumerState", "COMBINED_STREAM_KEY"]