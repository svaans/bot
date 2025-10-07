# core/data_feed.py
from __future__ import annotations

"""
DataFeed — Ingesta estable de velas cerradas con backfill y validación en streaming.

Compatibilidad esperada con el resto del bot:
- Atributos:
    .activos (propiedad booleana)
    ._managed_by_trader (flag opcional; el Trader puede marcarlo en tiempo de arranque)
    .ws_connected_event (asyncio.Event)
    .ws_failed_event (asyncio.Event)
- Métodos:
    precargar(symbols, cliente, minimo)
    escuchar(symbols, handler, cliente)
    iniciar() / start()
    detener()
    verificar_continuidad()

Notas:
- Valida integridad tanto en backfill como en streaming (duplicados, gaps, desalineados).
- Soporta stream por símbolo o combinado (si hay >1 símbolos).
- Colas por símbolo con política de backpressure configurable (block / drop_oldest).
- Watchdog de inactividad y de consumidores con reinicios automáticos.
- Exposición de eventos via `on_event(evt: str, data: dict)` para métricas/alertas.
"""

import asyncio
import contextlib
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional


COMBINED_STREAM_KEY = "__combined__"

from binance_api.websocket import (
    escuchar_velas,
    escuchar_velas_combinado,
    InactividadTimeoutError,
)
from binance_api.cliente import fetch_ohlcv_async
from core.utils.logger import configurar_logger
from core.utils.utils import (
    intervalo_a_segundos,
    timestamp_alineado,
    validar_integridad_velas,
)

UTC = timezone.utc
log = configurar_logger("datafeed", modo_silencioso=False)


class ConsumerState(Enum):
    STARTING = 0
    HEALTHY = 1
    STALLED = 2
    LOOP = 3


def _safe_float(value: Any, default: float) -> float:
    try:
        v = float(value)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


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
        max_reconnect_attempts: int | None = None,
        max_reconnect_time: float | None = None,
    ) -> None:
        # Config base
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

        # Estado interno
        self._running = False
        self._symbols: List[str] = []
        self._cliente: Any | None = None
        self._handler: Callable[[dict], Awaitable[None]] | None = None
        self._combined = False

        # Por símbolo
        self._queues: Dict[str, asyncio.Queue] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        self._last_close_ts: Dict[str, int] = {}          # último ts procesado/alineado
        self._ultimo_candle: Dict[str, dict] = {}         # último dict OHLCV (ligero)
        self._last_monotonic: Dict[str, float] = {}       # llegada de último candle a productor
        self._consumer_last: Dict[str, float] = {}        # último avance de consumer
        self._consumer_state: Dict[str, ConsumerState] = {}
        self._reconnect_attempts: Dict[str, int] = {}
        self._reconnect_since: Dict[str, float] = {}

        # Monitores
        self._monitor_inactividad_task: asyncio.Task | None = None
        self._monitor_consumers_task: asyncio.Task | None = None

        # Backfill
        self.min_buffer_candles = int(os.getenv("MIN_BUFFER_CANDLES", "30"))
        self._last_backfill_ts: Dict[str, int] = {}
        self._backfill_max = int(os.getenv("BACKFILL_MAX_CANDLES", "1000"))

        # Bandera opcional (la marca Trader para que el StartupManager no lo auto-arranque)
        self._managed_by_trader: bool = bool(getattr(self, "_managed_by_trader", False))

        # Estadísticas rápidas por símbolo
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._handler_expected_info: dict[str, int | str | None] | None = None
        self._handler_wrapper_calls: int = 0
        self._handler_log_events: int = 0

        # Señales de conexión del WebSocket (para StartupManager u otros coordinadores)
        self.ws_connected_event: asyncio.Event = asyncio.Event()

        self.ws_failed_event: asyncio.Event = asyncio.Event()
        self._ws_failure_reason: str | None = None

    # ─────────────────────── API pública ───────────────────────

    @property
    def activos(self) -> bool:
        """Indica si el feed tiene streams activos."""
        if not self._running:
            return False
        return any(not t.done() for t in self._tasks.values())

    def is_active(self) -> bool:
        """Alias de compat."""
        return self.activos

    def verificar_continuidad(self, *, max_gap_intervals: int | None = None) -> bool:
        """Verifica que el backfill previo no tenga huecos grandes."""
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
        """Backfill manual para `symbols` sin iniciar el stream."""
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
        """Inicia el stream por símbolo (o combinado si hay >1)."""
        await self.detener()

        # Nueva señal por cada arranque para evitar estados residuales sin perder referencias
        if isinstance(self.ws_connected_event, asyncio.Event):
            self.ws_connected_event.clear()
        else:  # pragma: no cover - compatibilidad defensiva
            self.ws_connected_event = asyncio.Event()
        if isinstance(self.ws_failed_event, asyncio.Event):
            self.ws_failed_event.clear()
        else:  # pragma: no cover - compatibilidad defensiva
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
            extra={
                "symbols": self._symbols,
                "intervalo": self.intervalo,
                "combined": self._combined,
            },
        )

        # Colas + consumers
        self._queues = {s: asyncio.Queue(maxsize=self.queue_max) for s in self._symbols}
        for s in self._symbols:
            self._set_consumer_state(s, ConsumerState.STARTING)
            self._consumer_tasks[s] = asyncio.create_task(self._consumer(s), name=f"consumer_{s}")

        # Monitores
        self._monitor_inactividad_task = asyncio.create_task(self._monitor_inactividad(), name="monitor_inactividad")
        self._monitor_consumers_task = asyncio.create_task(
            self._monitor_consumers(self.consumer_timeout), name="monitor_consumers"
        )

        # Streams
        if self._combined:
            tarea = asyncio.create_task(self._stream_combinado(self._symbols), name="stream_combinado")
            for s in self._symbols:
                self._tasks[s] = tarea
        else:
            for s in self._symbols:
                self._tasks[s] = asyncio.create_task(self._stream_simple(s), name=f"stream_{s}")

        # Espera a que terminen (o a detener())
        while self._running and any(not t.done() for t in self._tasks.values()):
            await asyncio.sleep(0.1)

    async def iniciar(self) -> None:
        """Alias cómodo cuando ya has configurado símbolos/handler/cliente."""
        if not self._symbols or not self._handler:
            log.warning("No se puede iniciar(): faltan símbolos o handler")
            return
        await self.escuchar(self._symbols, self._handler, cliente=self._cliente)

    async def start(self) -> None:
        """Compat en inglés."""
        await self.iniciar()

    async def detener(self) -> None:
        """Cancela tareas y limpia estado."""
        tasks: set[asyncio.Task] = set()
        tasks.update(self._tasks.values())
        tasks.update(self._consumer_tasks.values())
        if self._monitor_inactividad_task:
            tasks.add(self._monitor_inactividad_task)
        if self._monitor_consumers_task:
            tasks.add(self._monitor_consumers_task)

        for t in tasks:
            t.cancel()

        if tasks:
            done: set[asyncio.Task[Any]]
            pending: set[asyncio.Task[Any]]
            done, pending = await asyncio.wait(tasks, timeout=self.cancel_timeout)

            for task in done:
                try:
                    _ = task.result()
                except asyncio.CancelledError:
                    continue
                except Exception as exc:  # pragma: no cover - logging only
                    log.error(
                        "tarea_finalizo_con_error",
                        extra={
                            "task_name": task.get_name(),
                            "error": repr(exc),
                        },
                    )

            if pending:
                log.error(
                    "tareas_no_finalizaron_a_tiempo",
                    extra={
                        "timeout": self.cancel_timeout,
                        "pending_tasks": [t.get_name() for t in pending],
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

        # La señal se reinicia para futuros arranques manteniendo referencias existentes
        if isinstance(self.ws_connected_event, asyncio.Event):
            self.ws_connected_event.clear()
        else:  # pragma: no cover - compatibilidad defensiva
            self.ws_connected_event = asyncio.Event()
        if isinstance(self.ws_failed_event, asyncio.Event):
            self.ws_failed_event.clear()
        else:  # pragma: no cover - compatibilidad defensiva
            self.ws_failed_event = asyncio.Event()
        self._ws_failure_reason = None

    # ─────────────────────── Producción (WS) ───────────────────────

    async def _stream_simple(self, symbol: str) -> None:
        backoff = 0.5
        primera_vez = True
        while self._running and symbol in self._queues:
            try:
                if primera_vez:
                    await asyncio.sleep(random.random())  # desincroniza arranques
                    primera_vez = False

                await self._do_backfill(symbol)
                if not self.ws_connected_event.is_set():
                    self._signal_ws_connected(symbol)

                await escuchar_velas(
                    symbol,
                    self.intervalo,
                    lambda c: self._handle_candle(symbol, c),
                    {},
                    self.tiempo_inactividad,
                    60,
                    cliente=self._cliente,
                    mensaje_timeout=self.tiempo_inactividad,
                    backpressure=self.backpressure,
                    ultimo_timestamp=self._last_close_ts.get(symbol),
                    ultimo_cierre=(
                        self._ultimo_candle.get(symbol, {}).get("close")
                        if self._ultimo_candle.get(symbol)
                        else None
                    ),
                )
                log.info("%s: stream finalizado; reintentando…", symbol)
                self._emit("ws_end", {"symbol": symbol})
                if not self._register_reconnect_attempt(symbol, "stream_end"):
                    return
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

            except InactividadTimeoutError:
                log.warning("%s: reinicio por inactividad", symbol)
                self._emit("ws_inactividad", {"symbol": symbol})
                if not self.ws_connected_event.is_set():
                    self._signal_ws_failure("Timeout de inactividad durante el arranque del WS")
                    return
                if not self._register_reconnect_attempt(symbol, "inactividad"):
                    return
                backoff = 0.5
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.exception("%s: error en stream; reintentando", symbol)
                self._emit("ws_error", {"symbol": symbol, "error": str(e)})
                if not self.ws_connected_event.is_set():
                    self._signal_ws_failure(e)
                    return
                if not self._register_reconnect_attempt(symbol, "error"):
                    return
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

    async def _stream_combinado(self, symbols: List[str]) -> None:
        backoff = 0.5
        primera_vez = True
        while self._running and all(s in self._queues for s in symbols):
            try:
                if primera_vez:
                    await asyncio.sleep(random.random())
                    primera_vez = False

                await asyncio.gather(*(self._do_backfill(s) for s in symbols))
                if not self.ws_connected_event.is_set():
                    objetivo = symbols[0] if symbols else None
                    self._signal_ws_connected(objetivo)

                async def wrap(sym: str):
                    async def _w(c: dict) -> None:
                        await self._handle_candle(sym, c)
                    return _w

                handlers = {s: await wrap(s) for s in symbols}

                await escuchar_velas_combinado(
                    symbols,
                    self.intervalo,
                    handlers,
                    {},
                    self.tiempo_inactividad,
                    60,
                    cliente=self._cliente,
                    mensaje_timeout=self.tiempo_inactividad,
                    backpressure=self.backpressure,
                    ultimos={
                        s: {
                            "ultimo_timestamp": self._last_close_ts.get(s),
                            "ultimo_cierre": (
                                self._ultimo_candle.get(s, {}).get("close")
                                if self._ultimo_candle.get(s)
                                else None
                            ),
                        }
                        for s in symbols
                    },
                )
                log.info("stream combinado finalizado; reintentando…")
                self._emit("ws_end", {"symbols": symbols})
                if not self._register_reconnect_attempt(COMBINED_STREAM_KEY, "stream_end"):
                    return
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

            except InactividadTimeoutError:
                log.warning("stream combinado: reinicio por inactividad")
                self._emit("ws_inactividad", {"symbols": symbols})
                if not self.ws_connected_event.is_set():
                    self._signal_ws_failure("Timeout de inactividad durante el arranque del WS")
                    return
                if not self._register_reconnect_attempt(COMBINED_STREAM_KEY, "inactividad"):
                    return
                backoff = 0.5
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.exception("stream combinado: error; reintentando")
                self._emit("ws_error", {"symbols": symbols, "error": str(e)})
                if not self.ws_connected_event.is_set():
                    self._signal_ws_failure(e)
                    return
                if not self._register_reconnect_attempt(COMBINED_STREAM_KEY, "error"):
                    return
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

    # ─────────────────────── Cola y consumo ───────────────────────

    async def _handle_candle(self, symbol: str, candle: dict) -> None:
        """Callback desde el WS: filtra, valida y encola por símbolo."""
        # Solo velas cerradas y timestamps válidos/alineados
        if not candle.get("is_closed", True):
            return
        ts = candle.get("timestamp")
        if ts is None:
            for key in (
                "close_time",
                "closeTime",
                "open_time",
                "openTime",
                "event_time",
                "eventTime",
            ):
                candidato = candle.get(key)
                if candidato is not None:
                    ts = candidato
                    break

        try:
            ts = int(float(ts)) if ts is not None else None
        except (TypeError, ValueError):
            ts = None
        if ts is None or not timestamp_alineado(ts, self.intervalo):
            return
        

        # Normaliza el timestamp para mantener consistencia río abajo
        candle["timestamp"] = ts
        candle.setdefault("timeframe", self.intervalo)
        candle.setdefault("interval", self.intervalo)
        candle.setdefault("tf", self.intervalo)
        last = self._last_close_ts.get(symbol)
        if last is not None and ts <= last:
            # duplicado o out-of-order
            return

        self._reset_reconnect_tracking(symbol)
        if self._combined:
            self._reset_reconnect_tracking(COMBINED_STREAM_KEY)

        # Validación rápida de integridad (streaming) usando la propia vela
        # Espera dicts con al menos 'timestamp' y, si están presentes OHLCV, se validan.
        if not validar_integridad_velas(symbol, self.intervalo, [candle], log):
            self._emit("candle_invalid", {"symbol": symbol, "ts": ts})
            return

        if not self.ws_connected_event.is_set():
            self._signal_ws_connected(symbol)

        q = self._queues.get(symbol)
        if q is None:
            return

        candle.setdefault("_df_enqueue_time", time.monotonic())

        log.debug(
            "recv candle",
            extra={"symbol": symbol, "timestamp": ts, "queue_size": q.qsize()},
        )

        # Backpressure: block o drop_oldest
        if self.queue_policy == "block" or not q.maxsize:
            await q.put(candle)
        else:
            while True:
                try:
                    q.put_nowait(candle)
                    break
                except asyncio.QueueFull:
                    try:
                        _ = q.get_nowait()
                        q.task_done()
                        self._stats[symbol]["dropped"] += 1
                        self._emit("queue_drop", {"symbol": symbol})
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0)
                        continue

        self._stats[symbol]["received"] += 1
        self._last_close_ts[symbol] = ts
        self._ultimo_candle[symbol] = {
            k: candle.get(k) for k in ("timestamp", "open", "high", "low", "close", "volume")
        }
        self._last_monotonic[symbol] = time.monotonic()
        self._emit("tick", {"symbol": symbol, "ts": ts})

    async def _consumer(self, symbol: str) -> None:
        q = self._queues[symbol]
        self._consumer_last[symbol] = time.monotonic()
        self._set_consumer_state(symbol, ConsumerState.STARTING)

        while self._running:
            c = await q.get()
            sym = str(c.get("symbol") or symbol).upper()
            ts = c.get("timestamp") or c.get("close_time") or c.get("open_time")
            outcome = "ok"
            handler = self._handler
            if handler is None:
                log.error(
                    "handler.missing",
                    extra={
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "DataFeed._consumer",
                    },
                )
                q.task_done()
                continue
            handler_info = self._describe_handler(handler)
            if self._handler_expected_info and handler_info["id"] != self._handler_expected_info["id"]:
                log.warning(
                    "handler.mismatch",
                    extra={
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "DataFeed._consumer",
                        "expected_id": self._handler_expected_info["id"],
                        "expected_line": self._handler_expected_info.get("line"),
                        "actual_id": handler_info["id"],
                        "actual_line": handler_info.get("line"),
                    },
                )
            log.debug(
                "consumer.enter",
                extra={
                    "symbol": sym,
                    "timestamp": ts,
                    "stage": "DataFeed._consumer",
                    "handler_id": handler_info["id"],
                    "handler_qualname": handler_info.get("qualname"),
                    "handler_line": handler_info.get("line"),
                },
            )
            try:
                await asyncio.wait_for(handler(c), timeout=self.handler_timeout)
                self._set_consumer_state(symbol, ConsumerState.HEALTHY)
                self._stats[symbol]["processed"] += 1

            except asyncio.TimeoutError:
                outcome = "timeout"
                log.error("%s: handler superó %.3fs; vela omitida", symbol, self.handler_timeout)
                self._emit("handler_timeout", {"symbol": symbol})
                self._stats[symbol]["handler_timeouts"] += 1

            except asyncio.CancelledError:
                outcome = "cancelled"
                raise

            except Exception as e:
                outcome = "error"
                log.exception("%s: error procesando vela", symbol)
                self._emit("consumer_error", {"symbol": symbol, "error": str(e)})
                self._stats[symbol]["consumer_errors"] += 1

            finally:
                self._consumer_last[symbol] = time.monotonic()
                log.debug(
                    "consumer.exit",
                    extra={
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "DataFeed._consumer",
                        "result": outcome,
                        "handler_id": handler_info["id"],
                        "handler_wrapper_calls": self._handler_wrapper_calls,
                    },
                )
                q.task_done()
                if (
                    outcome == "ok"
                    and self._handler_log_events == 0
                    and not getattr(handler, "__df_debug_wrapper__", False)
                ):
                    self._activate_handler_debug_wrapper()
                # Seguridad: detectar loop de mismo timestamp
                ts = c.get("timestamp")
                prev = getattr(self, f"_last_processed_{symbol}", None)
                if ts is not None:
                    if prev is not None and ts <= prev:
                        self._set_consumer_state(symbol, ConsumerState.LOOP)
                        log.error("%s: consumer sin avanzar timestamp (prev=%s actual=%s)", symbol, prev, ts)
                        await self._reiniciar_consumer(symbol)
                        continue
                    setattr(self, f"_last_processed_{symbol}", ts)

    async def _reiniciar_consumer(self, symbol: str) -> None:
        t = self._consumer_tasks.get(symbol)
        if t and not t.done():
            t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(t, timeout=self.cancel_timeout)
        self._consumer_tasks[symbol] = asyncio.create_task(self._consumer(symbol), name=f"consumer_{symbol}")

    def _note_handler_log(self) -> None:
        """Registra que el handler final emitió logs propios."""

        self._handler_log_events += 1

    def _describe_handler(self, handler: Callable[[dict], Awaitable[None]]) -> dict[str, int | str | None]:
        """Extrae metadatos relevantes del handler para trazabilidad."""

        qualname = getattr(handler, "__qualname__", None)
        code = getattr(handler, "__code__", None)
        line = code.co_firstlineno if code else None
        module = getattr(handler, "__module__", None)
        return {
            "id": id(handler),
            "qualname": qualname,
            "line": line,
            "module": module,
        }

    def _activate_handler_debug_wrapper(self) -> None:
        """Envuelve el handler para instrumentar llamadas cuando falta evidencia de logs."""

        if self._handler is None:
            return

        if getattr(self._handler, "__df_debug_wrapper__", False):
            return

        original = self._handler
        handler_info = self._describe_handler(original)

        async def _wrapped(candle: dict) -> None:
            self._handler_wrapper_calls += 1
            call_idx = self._handler_wrapper_calls
            log.debug(
                "handler.wrapper.enter",
                extra={
                    "stage": "DataFeed._consumer",
                    "handler_id": handler_info["id"],
                    "handler_line": handler_info.get("line"),
                    "call": call_idx,
                },
            )
            try:
                await original(candle)
            finally:
                log.debug(
                    "handler.wrapper.exit",
                    extra={
                        "stage": "DataFeed._consumer",
                        "handler_id": handler_info["id"],
                        "handler_line": handler_info.get("line"),
                        "call": call_idx,
                    },
                )

        setattr(_wrapped, "__df_debug_wrapper__", True)
        setattr(_wrapped, "__df_original__", original)
        setattr(_wrapped, "__qualname__", getattr(original, "__qualname__", _wrapped.__qualname__))
        self._handler = _wrapped
        self._handler_expected_info = self._describe_handler(_wrapped)
        log.warning(
            "handler.wrapper.enabled",
            extra={
                "stage": "DataFeed._consumer",
                "handler_id": handler_info["id"],
                "handler_line": handler_info.get("line"),
                "reason": "missing_trader_logs",
            },
        )

    # ─────────────────────── Monitores ───────────────────────

    async def _monitor_inactividad(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.monitor_interval)
                ahora = time.monotonic()
                for s, last in list(self._last_monotonic.items()):
                    if last is None:
                        continue
                    if ahora - last > self.tiempo_inactividad:
                        log.warning("%s: stream inactivo; reiniciando…", s)
                        self._emit("ws_inactividad_watchdog", {"symbol": s})
                        await self._reiniciar_stream(s)
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("monitor_inactividad: error")

    async def _monitor_consumers(self, timeout: float | None = None) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.monitor_interval)
                ahora = time.monotonic()
                current_timeout = timeout if timeout is not None else self.consumer_timeout
                if current_timeout <= 0:
                    continue
                for s, last in list(self._consumer_last.items()):
                    tarea = self._consumer_tasks.get(s)
                    if tarea is None or tarea.done():
                        self._set_consumer_state(s, ConsumerState.STALLED)
                        log.warning("%s: consumer detenido; reiniciando…", s)
                        self._emit("consumer_stalled", {"symbol": s})
                        await self._reiniciar_consumer(s)
                        self._consumer_last[s] = time.monotonic()
                        continue
                    if ahora - last <= current_timeout:
                        continue
                    queue = self._queues.get(s)
                    if queue is None:
                        continue
                    if queue.empty():
                        stream_task = self._tasks.get(s)
                        stream_alive = stream_task is not None and not stream_task.done()
                        last_producer = self._last_monotonic.get(s)
                        producer_idle = (
                            last_producer is None
                            or ahora - last_producer > max(self.tiempo_inactividad, current_timeout)
                        )
                        if not stream_alive or producer_idle:
                            self._set_consumer_state(s, ConsumerState.STALLED)
                            log.warning("%s: consumer sin datos; reiniciando stream…", s)
                            self._emit("consumer_idle_no_data", {"symbol": s})
                            await self._reiniciar_stream(s)
                            self._consumer_last[s] = time.monotonic()
                        continue
                    self._set_consumer_state(s, ConsumerState.STALLED)
                    log.warning("%s: consumer sin progreso; reiniciando…", s)
                    self._emit("consumer_no_progress", {"symbol": s})
                    await self._reiniciar_consumer(s)
                    self._consumer_last[s] = time.monotonic()
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("monitor_consumers: error")

    async def _reiniciar_stream(self, symbol: str) -> None:
        if self._combined:
            task = next(iter(self._tasks.values()), None)
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(task, timeout=self.cancel_timeout)
            tarea = asyncio.create_task(self._stream_combinado(self._symbols), name="stream_combinado")
            for s in self._symbols:
                self._tasks[s] = tarea
        else:
            t = self._tasks.get(symbol)
            if t and not t.done():
                t.cancel()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(t, timeout=self.cancel_timeout)
            self._tasks[symbol] = asyncio.create_task(self._stream_simple(symbol), name=f"stream_{symbol}")

    # ─────────────────────── Backfill ───────────────────────

    async def _do_backfill(self, symbol: str) -> None:
        if not self._cliente:
            return

        intervalo_ms = self.intervalo_segundos * 1000
        ahora = int(datetime.now(UTC).timestamp() * 1000)
        last_ts = self._last_close_ts.get(symbol)
        cached = self._last_backfill_ts.get(symbol)

        # Evitar refetch redundante
        if last_ts is not None and cached is not None and last_ts <= cached:
            return

        if last_ts is None:
            faltan = self.min_buffer_candles
            since = ahora - intervalo_ms * faltan
        else:
            faltan = max(0, (ahora - last_ts) // intervalo_ms)
            if faltan <= 0:
                self._last_backfill_ts[symbol] = last_ts
                return
            since = last_ts + 1

        restante_total = min(max(faltan, self.min_buffer_candles), self._backfill_max)
        ohlcv: List[List[float]] = []

        while restante_total > 0:
            limit = min(100, restante_total)
            try:
                chunk = await fetch_ohlcv_async(self._cliente, symbol, self.intervalo, since=since, limit=limit)
            except Exception as e:
                log.exception("%s: error backfill (%s)", symbol, e)
                self._emit("backfill_error", {"symbol": symbol, "error": str(e)})
                return

            if not chunk:
                break

            ohlcv.extend(chunk)
            restante_total -= len(chunk)
            since = int(chunk[-1][0]) + 1
            if len(chunk) < limit:
                break

            await asyncio.sleep(0)

        if not ohlcv:
            return

        self._last_backfill_ts[symbol] = int(ohlcv[-1][0])

        # Validar integridad del bloque (backfill)
        ok = validar_integridad_velas(
            symbol,
            self.intervalo,
            ({"timestamp": int(o[0]), "open": o[1], "high": o[2], "low": o[3], "close": o[4], "volume": o[5]} for o in ohlcv),
            log,
        )
        if not ok:
            self._emit("backfill_invalid", {"symbol": symbol})

        # Inyectar el backfill a la cola como si fuese streaming (is_closed=True)
        for o in ohlcv:
            c = {
                "symbol": symbol,
                "timestamp": int(o[0]),
                "open": _safe_float(o[1], 0.0),
                "high": _safe_float(o[2], 0.0),
                "low": _safe_float(o[3], 0.0),
                "close": _safe_float(o[4], 0.0),
                "volume": _safe_float(o[5], 0.0),
                "is_closed": True,
            }
            await self._handle_candle(symbol, c)

        self._emit("backfill_ok", {"symbol": symbol, "candles": len(ohlcv)})

    # ─────────────────────── Utilidades ───────────────────────

    def _signal_ws_connected(self, symbol: str | None = None) -> None:
        if self.ws_connected_event.is_set():
            return
        self.ws_connected_event.set()
        payload = {"intervalo": self.intervalo}
        if symbol is not None:
            payload["symbol"] = symbol
        self._emit("ws_connected", payload)

    def _signal_ws_failure(self, error: BaseException | str | None = None) -> None:
        if self.ws_failed_event.is_set():
            return
        reason: str | None
        if isinstance(error, BaseException):
            reason = f"{error.__class__.__name__}: {error}"
        elif error:
            reason = str(error)
        else:
            reason = None
        if not reason:
            reason = "error de conexión no especificado"
        self._ws_failure_reason = reason
        self.ws_failed_event.set()
        payload = {"intervalo": self.intervalo}
        if reason:
            payload["reason"] = reason
        self._emit("ws_connect_failed", payload)

    def _register_reconnect_attempt(self, key: str, reason: str) -> bool:
        if self.max_reconnect_attempts is None and self.max_reconnect_time is None:
            return True

        now = time.monotonic()
        attempts = self._reconnect_attempts.get(key, 0) + 1
        self._reconnect_attempts[key] = attempts
        since = self._reconnect_since.get(key)
        if since is None:
            since = now
            self._reconnect_since[key] = since
        elapsed = max(0.0, now - since)

        payload = {
            "key": key,
            "attempts": attempts,
            "elapsed": elapsed,
            "reason": reason,
        }

        if self.max_reconnect_attempts is not None and attempts > self.max_reconnect_attempts:
            payload["max_attempts"] = self.max_reconnect_attempts
            log.error(
                "ws.max_retries_exceeded",
                extra={
                    "key": key,
                    "attempts": attempts,
                    "max_attempts": self.max_reconnect_attempts,
                    "reason": reason,
                },
            )
            self._emit("ws_retries_exceeded", payload)
            self._signal_ws_failure(
                f"Se superó el máximo de reintentos permitidos ({self.max_reconnect_attempts})"
            )
            return False

        if self.max_reconnect_time is not None and elapsed >= self.max_reconnect_time:
            payload["max_downtime"] = self.max_reconnect_time
            log.error(
                "ws.downtime_exceeded",
                extra={
                    "key": key,
                    "elapsed": elapsed,
                    "max_downtime": self.max_reconnect_time,
                    "reason": reason,
                },
            )
            self._emit("ws_downtime_exceeded", payload)
            self._signal_ws_failure(
                f"Se superó el máximo de tiempo sin conexión ({self.max_reconnect_time:.1f}s)"
            )
            return False

        self._emit("ws_retry", payload)
        return True

    def _reset_reconnect_tracking(self, key: str) -> None:
        if key in self._reconnect_attempts:
            self._reconnect_attempts.pop(key, None)
        if key in self._reconnect_since:
            self._reconnect_since.pop(key, None)

    @property
    def ws_failure_reason(self) -> str | None:
        return self._ws_failure_reason

    def _set_consumer_state(self, symbol: str, state: ConsumerState) -> None:
        prev = self._consumer_state.get(symbol)
        if prev == state:
            return
        self._consumer_state[symbol] = state
        self._emit("consumer_state", {"symbol": symbol, "state": state.name.lower()})

    def _emit(self, evento: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evento, data)
            except Exception:
                # Evitar que un callback de métricas rompa el feed
                log.exception("on_event lanzó excepción")

    def stats(self) -> Dict[str, Dict[str, int]]:
        """Devuelve estadísticas de velas recibidas/procesadas/dropeadas/errores."""
        return {s: dict(v) for s, v in self._stats.items()}



