from __future__ import annotations

"""Implementación principal del ``DataFeed`` modularizado."""

import asyncio
import contextlib
import inspect
import logging
import os
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional

from core.utils.log_utils import log_kv, safe_extra
from core.utils.utils import intervalo_a_segundos

from . import backfill as backfill_module
from . import events as events_module
from . import handlers as handlers_module
from . import monitors as monitors_module
from . import streaming as streaming_module
from ._shared import COMBINED_STREAM_KEY, ConsumerState, _safe_float, _safe_int, log
from .spread_sampler import SpreadSampler


class DataFeed:
    """DataFeed robusto y legible para velas cerradas."""

    def __init__(
        self,
        intervalo: str,
        *,
        handler_timeout: float = float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
        inactivity_intervals: int = int(os.getenv("DF_INACTIVITY_INTERVALS", "10")),
        queue_max: int = int(os.getenv("DF_QUEUE_MAX", "2000")),
        queue_policy: str | None = None,
        monitor_interval: float = float(os.getenv("DF_MONITOR_INTERVAL", "5")),
        queue_min_recommended: int | None = None,
        consumer_timeout_intervals: float | None = None,
        consumer_timeout_min: float | None = None,
        backpressure: bool = os.getenv("DF_BACKPRESSURE", "true").lower() == "true",
        cancel_timeout: float = float(os.getenv("DF_CANCEL_TIMEOUT", "5")),
        on_event: Optional[Callable[[str, dict], None]] = None,
        event_bus: Any | None = None,
        max_reconnect_attempts: int | None = None,
        max_reconnect_time: float | None = None,
        queue_autotune: bool | None = None,
        queue_burst_factor: float | None = None,
        max_event_delay_seconds: float | None = None,
    ) -> None:
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)

        self.handler_timeout = max(0.1, float(handler_timeout))
        self.inactivity_intervals = max(1, int(inactivity_intervals))
        self.tiempo_inactividad = self.intervalo_segundos * self.inactivity_intervals

        if queue_policy is None:
            queue_policy = os.getenv("DF_QUEUE_POLICY", "drop_oldest")
        allowed_policies = {"drop_oldest", "block", "drop_newest"}
        parsed_policy = str(queue_policy).lower()
        self.queue_policy = parsed_policy if parsed_policy in allowed_policies else "drop_oldest"

        self.queue_max = max(0, int(queue_max))
        self._external_on_event = on_event
        self.on_event = self._handle_feed_event

        env_queue_min = max(0, _safe_int(os.getenv("DF_QUEUE_MIN_RECOMMENDED", "16"), 16))
        parsed_min = (
            max(0, _safe_int(queue_min_recommended, env_queue_min))
            if queue_min_recommended is not None
            else env_queue_min
        )
        self.queue_min_recommended = max(0, parsed_min)
        self._queue_capacity_warning_emitted = False
        self._queue_capacity_breach_logged = False
        if 0 < self.queue_max < self.queue_min_recommended:
            payload = {
                "intervalo": self.intervalo,
                "queue_max": self.queue_max,
                "queue_min_recommended": self.queue_min_recommended,
                "queue_policy": self.queue_policy,
            }
            log.warning("queue.capacity.low_config", extra=safe_extra(payload))
            events_module.emit_event(self, "queue_capacity_below_recommended", payload)
            self._queue_capacity_warning_emitted = True

        if queue_autotune is None:
            queue_autotune = os.getenv("DF_QUEUE_AUTOTUNE", "true").lower() == "true"
        self.queue_autotune = bool(queue_autotune)
        if queue_burst_factor is None:
            queue_burst_factor = _safe_float(os.getenv("DF_QUEUE_BURST_FACTOR", "4.0"), 4.0)
        self.queue_burst_factor = max(1.0, float(queue_burst_factor))
        self._queue_autotune_applied = False

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
        self._event_bus: Any | None = None
        self._event_bus_registered_events: set[str] = set()
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
        self._ws_retry_telemetry: Dict[str, dict[str, Any]] = {}

        spread_enabled_env = os.getenv("DF_SPREAD_ENABLED", "true").strip().lower()
        self._spread_enabled = spread_enabled_env not in {"0", "false", "no"}
        spread_ttl = _safe_float(os.getenv("DF_SPREAD_CACHE_TTL", "2.5"), 2.5)
        self._spread_sampler: SpreadSampler | None = None
        if self._spread_enabled:
            try:
                self._spread_sampler = SpreadSampler(ttl=max(0.25, spread_ttl))
            except Exception:
                log.exception("spread.sampler_init_failed", extra=safe_extra({"stage": "DataFeed"}))
                self._spread_enabled = False
        self._spread_fetcher: Any | None = None
        self._spread_fetcher_unavailable = False
        self._spread_fetcher_error_logged = False

        self.ws_backoff_base = max(0.05, _safe_float(os.getenv("DF_WS_BACKOFF_BASE", "0.5"), 0.5))
        self.ws_backoff_max = max(
            self.ws_backoff_base,
            _safe_float(os.getenv("DF_WS_BACKOFF_MAX", "60.0"), 60.0),
        )
        self.ws_backoff_jitter = max(0.0, _safe_float(os.getenv("DF_WS_BACKOFF_JITTER", "0.25"), 0.25))

        if max_event_delay_seconds is None:
            max_event_delay_seconds = _safe_float(os.getenv("DF_MAX_EVENT_DELAY_SEC", "0"), 0.0)
        self._max_event_delay_ms: float | None
        if max_event_delay_seconds is None or max_event_delay_seconds <= 0:
            self._max_event_delay_ms = None
        else:
            self._max_event_delay_ms = float(max_event_delay_seconds) * 1000.0
            
        self._monitor_inactividad_task: asyncio.Task | None = None
        self._monitor_consumers_task: asyncio.Task | None = None

        self.min_buffer_candles = int(os.getenv("MIN_BUFFER_CANDLES", "30"))
        self._last_backfill_ts: Dict[str, int] = {}
        self._backfill_max = int(os.getenv("BACKFILL_MAX_CANDLES", "1000"))
        self._backfill_ventana_enabled = os.getenv("BACKFILL_VENTANA_ENABLED", "false").lower() == "true"
        raw_window = os.getenv("BACKFILL_VENTANA_WINDOW")
        try:
            self._backfill_window_target = int(raw_window) if raw_window is not None else self.min_buffer_candles
        except (TypeError, ValueError):
            self._backfill_window_target = self.min_buffer_candles
        self._backfill_window_targets: Dict[str, int] = {}
        self._backfill_window_drop_balance: Dict[str, int] = defaultdict(int)

        self._managed_by_trader: bool = bool(getattr(self, "_managed_by_trader", False))

        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._handler_expected_info: dict[str, int | str | None] | None = None
        self._handler_wrapper_calls: int = 0
        self._handler_log_events: int = 0

        raw_backfill_threshold = _safe_float(
            os.getenv("DF_BACKFILL_LATENCY_THRESHOLD_SEC", "0"),
            0.0,
        )
        self._backfill_latency_threshold: float | None = (
            raw_backfill_threshold if raw_backfill_threshold > 0 else None
        )

        if self.backpressure and self.queue_autotune:
            self._apply_queue_backpressure_strategy()

        # ``precargar`` modifica temporalmente el estado interno del feed; el lock
        # asegura que no se ejecute concurrentemente con otras llamadas que
        # mutan ``_symbols`` o ``_cliente``.
        self._precargar_lock = asyncio.Lock()

        self.ws_connected_event: asyncio.Event = asyncio.Event()
        self.ws_failed_event: asyncio.Event = asyncio.Event()
        self._ws_failure_reason: str | None = None

    def _apply_queue_backpressure_strategy(self) -> None:
        """Ajusta la cola para escenarios con bursts elevados y backpressure."""

        original_policy = self.queue_policy
        original_max = self.queue_max

        target_base = self.queue_min_recommended or self.queue_max or 16
        target_capacity = max(int(target_base), int(target_base * self.queue_burst_factor))
        target_capacity = max(1, target_capacity)

        changes: Dict[str, Any] = {}

        if self.queue_policy == "block":
            if self.queue_max <= 0 or self.queue_max < target_capacity:
                self.queue_max = target_capacity
                changes["queue_max"] = self.queue_max
        else:
            if self.queue_max <= 0:
                self.queue_policy = "block"
                changes["queue_policy"] = self.queue_policy
                self.queue_max = target_capacity
                changes["queue_max"] = self.queue_max
            elif 0 < self.queue_max < max(1, self.queue_min_recommended):
                self.queue_policy = "block"
                changes["queue_policy"] = self.queue_policy
                if self.queue_max < target_capacity:
                    self.queue_max = target_capacity
                    changes["queue_max"] = self.queue_max
            elif self.queue_max < target_capacity:
                self.queue_max = target_capacity
                changes["queue_max"] = self.queue_max

        self._queue_autotune_applied = bool(changes)

        if not changes:
            return

        payload = {
            "intervalo": self.intervalo,
            "original_policy": original_policy,
            "new_policy": self.queue_policy,
            "original_max": original_max,
            "new_max": self.queue_max,
            "burst_factor": self.queue_burst_factor,
            **changes,
        }
        log.info("queue.autotune", extra=safe_extra(payload))
        events_module.emit_event(self, "queue_autotune", payload)

    @property
    def event_bus(self) -> Any | None:
        return self._event_bus

    @event_bus.setter
    def event_bus(self, value: Any | None) -> None:
        previous = getattr(self, "_event_bus", None)
        self._event_bus = value
        if value is None:
            self._event_bus_registered_events.clear()
            return
        if value is not previous:
            self._event_bus_registered_events.clear()
        start = getattr(value, "start", None)
        if callable(start):
            try:
                start()
            except Exception:
                log.warning(
                    "No se pudo iniciar event_bus tras inyección en DataFeed",
                    exc_info=True,
                )
        self._register_event_bus_handlers()

    def _handle_feed_event(self, event: str, data: dict[str, Any]) -> None:
        bus = getattr(self, "_event_bus", None)
        if bus is not None:
            emitter = getattr(bus, "emit", None)
            if callable(emitter):
                try:
                    payload_for_bus = dict(data)
                    payload_for_bus.setdefault("intervalo", self.intervalo)
                    emitter(event, payload_for_bus)
                except Exception:
                    log.debug(
                        "No se pudo reenviar evento %s al event_bus", event, exc_info=True
                    )
        if self._external_on_event is not None:
            try:
                self._external_on_event(event, data)
            except Exception:
                log.exception("on_event externo lanzó excepción")

    def _register_event_bus_handlers(self) -> None:
        bus = self._event_bus
        if bus is None:
            return
        subscribe = getattr(bus, "subscribe", None)
        if not callable(subscribe):
            return
        if "queue_drop" in self._event_bus_registered_events:
            return
        try:
            subscribe("queue_drop", self._on_queue_drop_event)
            self._event_bus_registered_events.add("queue_drop")
        except Exception:
            log.warning(
                "No se pudo suscribir a queue_drop en event_bus", exc_info=True
            )

    async def _on_queue_drop_event(self, payload: Any | None) -> None:
        if not isinstance(payload, dict):
            return
        symbol = payload.get("symbol")
        if not symbol:
            return
        count_raw = payload.get("count", 1)
        try:
            count = int(count_raw)
        except (TypeError, ValueError):
            count = 1
        if count <= 0:
            count = 1
        self._increase_backfill_drop(str(symbol).upper(), count)

    def _increase_backfill_drop(self, symbol: str, count: int) -> None:
        if count <= 0:
            return
        base_target = int(self._backfill_window_target or self.min_buffer_candles)
        balance = self._backfill_window_drop_balance.get(symbol, 0) + count
        self._backfill_window_drop_balance[symbol] = balance
        new_target = min(self._backfill_max, max(base_target, base_target + balance))
        self._backfill_window_targets[symbol] = new_target
        log.debug(
            "backfill.window.adjust",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "base": base_target,
                    "balance": balance,
                    "target": new_target,
                }
            ),
        )

    def _resolve_backfill_window_target(self, symbol: str | None = None) -> int:
        base_target = int(self._backfill_window_target or self.min_buffer_candles)
        if symbol:
            dynamic = self._backfill_window_targets.get(str(symbol).upper())
            if dynamic is not None:
                return dynamic
        return base_target

    def _note_backfill_window_recovery(self, symbol: str, recovered: int) -> None:
        if recovered <= 0:
            return
        sym_key = str(symbol).upper()
        if not sym_key:
            return
        balance = self._backfill_window_drop_balance.get(sym_key, 0)
        if balance <= 0:
            return
        remaining = max(0, balance - recovered)
        if remaining == 0:
            self._backfill_window_drop_balance.pop(sym_key, None)
            self._backfill_window_targets.pop(sym_key, None)
            log.debug(
                "backfill.window.recovered",
                extra=safe_extra({"symbol": sym_key, "recovered": recovered}),
            )
            return
        self._backfill_window_drop_balance[sym_key] = remaining
        base_target = int(self._backfill_window_target or self.min_buffer_candles)
        new_target = min(self._backfill_max, max(base_target, base_target + remaining))
        self._backfill_window_targets[sym_key] = new_target
        log.debug(
            "backfill.window.balance",
            extra=safe_extra(
                {
                    "symbol": sym_key,
                    "balance": remaining,
                    "target": new_target,
                    "recovered": recovered,
                }
            ),
        )

    @property
    def connected(self) -> bool:
        """Indica si el DataFeed reporta conexión activa al WebSocket."""

        evt = getattr(self, "ws_connected_event", None)
        if isinstance(evt, asyncio.Event):
            return evt.is_set()
        maybe_is_set = getattr(evt, "is_set", None)
        if callable(maybe_is_set):
            with contextlib.suppress(Exception):
                return bool(maybe_is_set())
        return False

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

        async with self._precargar_lock:
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
        handler_payload = {k: v for k, v in handler_info.items() if v is not None}
        log_kv(
            log,
            logging.DEBUG,
            "handler.registered",
            stage="DataFeed.escuchar",
            **handler_payload,
        )

        self._handler = handler
        self._cliente = cliente
        self._combined = len(self._symbols) > 1
        self._running = True
        self._reconnect_attempts.clear()
        self._reconnect_since.clear()
        self._ws_retry_telemetry.clear()

        log.info(
            "suscrito",
            extra=safe_extra(
                {"symbols": self._symbols, "intervalo": self.intervalo, "combined": self._combined}
            ),
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

    async def ensure_running(self) -> None:
        """Arranca el feed sólo si no se encuentra en ejecución."""

        if getattr(self, "_running", False):
            return

        start_fn = getattr(self, "start", None) or getattr(self, "iniciar", None)
        if not callable(start_fn):
            log.debug("ensure_running: sin start() disponible; se omite")
            return

        try:
            result = start_fn()
        except TypeError:
            # Soporta métodos síncronos sin argumentos opcionales adicionales.
            result = None

        if inspect.isawaitable(result):
            await result
        else:
            # Garantiza que las tareas internas se programen.
            await asyncio.sleep(0)

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
                        extra=safe_extra(
                            {"task_name": task.get_name(), "error": repr(exc)}
                        ),
                    )

            if pending:
                log.error(
                    "tareas_no_finalizaron_a_tiempo",
                    extra=safe_extra(
                        {
                            "timeout": self.cancel_timeout,
                            "pending_tasks": [task.get_name() for task in pending],
                        }
                    ),
                )
                for task in pending:
                    task.cancel()

                gathered = await asyncio.gather(*pending, return_exceptions=True)
                for task, result in zip(pending, gathered):
                    if isinstance(result, asyncio.CancelledError):
                        continue
                    if isinstance(result, Exception):
                        log.error(
                            "tarea_pendiente_finalizo_con_error",
                            extra=safe_extra(
                                {
                                    "task_name": task.get_name(),
                                    "error": repr(result),
                                }
                            ),
                        )

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
        self._ws_retry_telemetry.clear()

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

    def _resolve_spread_fetcher(self) -> Callable[[Any | None, str], Awaitable[Mapping[str, Any] | None]] | None:
        if not self._spread_enabled or self._spread_sampler is None:
            return None
        if self._spread_fetcher is not None:
            return self._spread_fetcher  # type: ignore[return-value]
        if self._spread_fetcher_unavailable:
            return None
        try:
            from binance_api.cliente import fetch_book_ticker_async
        except Exception:
            if not self._spread_fetcher_error_logged:
                log.warning(
                    "spread.fetcher_unavailable",
                    extra=safe_extra({"stage": "DataFeed", "reason": "import_error"}),
                )
                self._spread_fetcher_error_logged = True
            self._spread_fetcher_unavailable = True
            return None
        self._spread_fetcher = fetch_book_ticker_async
        return fetch_book_ticker_async

    async def _maybe_attach_spread(self, symbol: str, candle: dict) -> None:
        if not self._spread_enabled or self._spread_sampler is None:
            return
        if not isinstance(candle, dict):
            return

        existing = candle.get("spread_ratio")
        if existing is None:
            existing = candle.get("spread")
        if existing is not None:
            try:
                candle.setdefault("spread_ratio", float(existing))
            except (TypeError, ValueError):
                candle.pop("spread_ratio", None)
            return

        fetcher = self._resolve_spread_fetcher()
        if fetcher is None:
            return

        cliente = getattr(self, "_cliente", None)
        try:
            sample = await self._spread_sampler.sample(symbol, fetcher, cliente=cliente)
        except Exception:
            log.exception(
                "spread.sample_error",
                extra=safe_extra({"symbol": symbol, "stage": "DataFeed"}),
            )
            return

        if sample is None:
            return

        candle["spread_ratio"] = sample.ratio
        candle.setdefault("spread", sample.ratio)
        candle.setdefault("best_bid", sample.bid)
        candle.setdefault("best_ask", sample.ask)
        if sample.timestamp_ms is not None:
            candle.setdefault("spread_timestamp", sample.timestamp_ms)
        if sample.source:
            candle.setdefault("spread_source", sample.source)

    async def _do_backfill(self, symbol: str) -> None:
        start = time.perf_counter()
        await backfill_module.do_backfill(self, symbol)
        elapsed = max(0.0, time.perf_counter() - start)
        self._stats[symbol]["backfill_last_elapsed_ms"] = int(elapsed * 1000)
        threshold = self._backfill_latency_threshold
        if threshold is not None and elapsed >= threshold:
            stats_snapshot = {k: int(v) for k, v in self._stats[symbol].items()}
            payload = {
                "symbol": symbol,
                "timeframe": self.intervalo,
                "elapsed": elapsed,
                "elapsed_ms": int(elapsed * 1000),
                "threshold": threshold,
                "stats": stats_snapshot,
            }
            events_module.emit_bus_signal(self, "backfill.latency", payload)

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

    def _verify_reconnect_limits(self, key: str, reason: str) -> bool:
        return events_module.verify_reconnect_limits(self, key, reason)

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
