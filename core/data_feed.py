"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
import json
import random
import os
from typing import Awaitable, Callable, Dict, Iterable, Any, Deque, Literal, Tuple
from datetime import datetime, timezone, timedelta
import time
from collections import deque, defaultdict
from enum import Enum
from uuid import uuid4
from binance_api.websocket import (
    escuchar_velas,
    escuchar_velas_combinado,
    InactividadTimeoutError,
)
from binance_api.cliente import fetch_ohlcv_async
from core.utils.logger import configurar_logger, _should_log
from core.utils import intervalo_a_segundos, validar_integridad_velas, timestamp_alineado
from core.utils.warmup import splice_with_last
from core.utils.backoff import calcular_backoff
from core.registro_metrico import registro_metrico
from core.metrics import (
    QUEUE_SIZE,
    INGEST_LATENCY,
    registrar_watchdog_restart,
    registrar_vela_rechazada,
)
from observabilidad import metrics as obs_metrics
from observabilidad.perf_tracker import StageMetrics
from core.supervisor import (
    tick,
    tick_data,
    supervised_task,
    registrar_reinicio_inactividad,
    registrar_reconexion_datafeed,
    beat,
    task_cooldown,
)
from core.notification_manager import (
    NotificationManager,
    crear_notification_manager_desde_env,
)
from ccxt.base.errors import AuthenticationError, NetworkError
from config.config import BACKFILL_MAX_CANDLES

UTC = timezone.utc
log = configurar_logger('datafeed', modo_silencioso=False)

class ConsumerState(Enum):
    """Estados posibles del consumidor de colas del ``DataFeed``."""

    STARTING = 0
    HEALTHY = 1
    STALLED = 2
    LOOP = 3


class ConsumerStalledError(RuntimeError):
    """Señala que el consumidor no avanza y debe reiniciarse."""


class ConsumerProgressError(RuntimeError):
    """Señala que el consumidor procesa velas sin avanzar el timestamp."""

class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(
        self,
        intervalo: str,
        monitor_interval: int = 5,
        max_restarts: int = 10,
        inactivity_intervals: int = 10,
        handler_timeout: float = float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
        cancel_timeout: float = 5,
        backpressure: bool = False,
        drop_oldest: bool | None = None,
        batch_size: int | None = None,
        reset_cb: Callable[[str], None] | None = None,
        notification_manager: NotificationManager | None = None,
        notifications_enabled: bool | None = None,
    ) -> None:
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)
        self.inactivity_intervals = inactivity_intervals
        self.tiempo_inactividad = (
            self.intervalo_segundos * self.inactivity_intervals
        )
        self.cancel_timeout = cancel_timeout
        self.ping_interval = 60  # frecuencia fija de ping en segundos
        self.monitor_interval = max(1, monitor_interval)
        self.max_stream_restarts = max_restarts
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._last_close_ts: Dict[str, int] = {}
        self._monitor_global_task: asyncio.Task | None = None
        self._ultimo_candle: Dict[str, dict] = {}
        self._handler_actual: Callable[[dict], Awaitable[None]] | None = None
        self._running = False
        self._cliente: Any | None = None
        if notifications_enabled is None:
            notifications_enabled = (
                os.getenv("DF_NOTIFICATIONS", "true").lower() == "true"
            )
        if notification_manager is not None:
            self.notificador = notification_manager
        elif notifications_enabled:
            self.notificador = crear_notification_manager_desde_env()
        else:
            self.notificador = None
        self._symbols: list[str] = []
        self.reinicios_forzados_total = 0
        self.handler_timeout = handler_timeout
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._reiniciando: set[str] = set()
        self._handler_timeouts: Dict[str, int] = {}
        self._mensajes_recibidos: Dict[str, int] = {}
        self._queue_discards: Dict[str, int] = {}
        self._coalesce_counts: Dict[str, int] = {}
        self._reinicios_inactividad: Dict[str, int] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._consumer_last: Dict[str, float] = {}
        self._monitor_consumers_task: asyncio.Task | None = None
        self._queue_default_limit = int(os.getenv("DF_QUEUE_MAX", "2000"))
        self._queue_limits: Dict[str, int] = {}
        self._queue_policy_default = os.getenv("DF_QUEUE_POLICY", "block").lower()
        self._queue_policy_overrides: Dict[str, str] = {}
        self._queue_safety_policy = os.getenv("DF_QUEUE_SAFETY_POLICY", "drop_oldest").lower()
        self._coalesce_window_ms = max(0, int(os.getenv("DF_QUEUE_COALESCE_MS", "0")))
        self._queue_high_watermark = float(os.getenv("DF_QUEUE_HIGH_WATERMARK", "0.8"))
        self._queue_alert_interval = float(os.getenv("DF_QUEUE_ALERT_INTERVAL", "5.0"))
        self._metrics_log_interval = float(os.getenv("DF_METRICS_LOG_INTERVAL", "5.0"))
        self._queue_alert_last: Dict[str, float] = {}
        self._stage_metrics: Dict[str, StageMetrics] = {}
        if backpressure is None:
            self.backpressure = (
                os.getenv("DF_BACKPRESSURE", "true").lower() == "true"
            )
        else:
            self.backpressure = backpressure
        if drop_oldest is None:
            self.drop_oldest = (
                os.getenv("DF_BACKPRESSURE_DROP", "true").lower() == "true"
            )
        else:
            self.drop_oldest = drop_oldest
        self.configure_queue_control()
        if not self.backpressure and self._queue_policy_default == "block":
            self._queue_policy_default = "drop_oldest" if self.drop_oldest else "reject"
        registrar_reconexion_datafeed(self._reconectar_por_supervisor)
        self._stream_restart_stats: Dict[str, Deque[float]] = {}
        self.min_buffer_candles = int(os.getenv("MIN_BUFFER_CANDLES", "30"))
        # Evita repetir llamadas de backfill para el mismo timestamp
        self._last_backfill_ts: Dict[str, int] = {}
        self._combined = False
        self._reset_cb = reset_cb
        # Configuración de batch y métricas
        env_batch = int(os.getenv("BATCH_SIZE_CONSUMER", "3"))
        if batch_size is not None:
            self.batch_size = max(1, batch_size)
        else:
            self.batch_size = max(1, env_batch)
        self._producer_stats: Dict[str, Dict[str, float]] = {}
        self._queue_windows: Dict[str, list] = {}
        self._last_window_reset: Dict[str, float] = {}
        self._estado: Dict[str, Any] | None = None
        self._backfill_retry_tasks: Dict[str, asyncio.Task] = {}
        self._backfill_retry_last: Dict[str, float] = {}
        self._backfill_retry_delay = float(os.getenv("BACKFILL_RETRY_DELAY", "1.0"))
        self._consumer_rate_window = max(
            1.0, float(os.getenv("DF_CONSUMER_RATE_WINDOW", "10.0"))
        )
        self._consumer_min_window_span = 1.0
        self._consumer_rate_epsilon = 1e-6
        self._consumer_history: Dict[str, Deque[Tuple[float, int]]] = defaultdict(deque)
        self._consumer_window_totals: Dict[str, int] = defaultdict(int)
        self._consumer_last_rate: Dict[str, float] = defaultdict(float)
        self._consumer_last_log: Dict[str, float] = {}
        self._consumer_prev_qsize: Dict[str, int] = {}
        self._consumer_status: Dict[str, ConsumerState] = {}
        self._consumer_state_reason: Dict[str, str] = {}
        self._consumer_last_processed_ts: Dict[str, int] = {}
        self._consumer_counter_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def configure_queue_control(
        self,
        *,
        default_limit: int | None = None,
        per_symbol_limits: Dict[str, int] | None = None,
        default_policy: str | None = None,
        policy_overrides: Dict[str, str] | None = None,
        high_watermark: float | None = None,
        coalesce_window_ms: int | None = None,
        safety_policy: str | None = None,
        alert_interval: float | None = None,
        metrics_log_interval: float | None = None,
    ) -> None:
        """Actualiza la configuración de control de colas del ``DataFeed``."""

        if default_limit is not None:
            self._queue_default_limit = max(0, int(default_limit))
        if per_symbol_limits is not None:
            self._queue_limits = {
                sym.upper(): max(0, int(limit))
                for sym, limit in per_symbol_limits.items()
            }
        if default_policy is not None:
            self._queue_policy_default = default_policy.lower()
        if policy_overrides is not None:
            self._queue_policy_overrides = {
                sym.upper(): policy.lower()
                for sym, policy in policy_overrides.items()
            }
        if safety_policy is not None:
            self._queue_safety_policy = safety_policy.lower()
        if coalesce_window_ms is not None:
            self._coalesce_window_ms = max(0, int(coalesce_window_ms))
        if high_watermark is not None:
            self._queue_high_watermark = max(0.0, min(1.0, float(high_watermark)))
        if alert_interval is not None:
            self._queue_alert_interval = max(0.5, float(alert_interval))
        if metrics_log_interval is not None:
            self._metrics_log_interval = max(0.5, float(metrics_log_interval))

    def _queue_maxsize_for(self, symbol: str) -> int:
        return max(0, self._queue_limits.get(symbol.upper(), self._queue_default_limit))

    def _queue_policy_for(self, symbol: str) -> str:
        return self._queue_policy_overrides.get(symbol.upper(), self._queue_policy_default)

    def _get_stage_metrics(self, symbol: str) -> StageMetrics:
        metrics = self._stage_metrics.get(symbol)
        if metrics is None:
            metrics = StageMetrics(
                "datafeed_consumer",
                log_interval=self._metrics_log_interval,
            )
            self._stage_metrics[symbol] = metrics
        return metrics

    def _emit_high_watermark(self, symbol: str, qsize: int, maxsize: int) -> None:
        if maxsize <= 0:
            return
        ratio = qsize / maxsize
        if ratio < self._queue_high_watermark:
            return
        ahora = time.monotonic()
        last = self._queue_alert_last.get(symbol, 0.0)
        if ahora - last < self._queue_alert_interval:
            return
        self._queue_alert_last[symbol] = ahora
        payload = {
            "evento": "queue_high_watermark",
            "symbol": symbol,
            "stage": "datafeed",
            "queue_size": qsize,
            "queue_max": maxsize,
            "policy": self._queue_policy_for(symbol),
            "ratio": round(ratio, 4),
        }
        log.warning(json.dumps(payload, ensure_ascii=False))
        obs_metrics.QUEUE_HIGH_WATERMARK_RATIO.labels(symbol=symbol).set(ratio)
        registro_metrico.registrar(
            "queue_high_watermark",
            {"symbol": symbol, "qsize": qsize, "maxsize": maxsize},
        )

    def _drop_oldest(self, symbol: str, queue: asyncio.Queue) -> bool:
        try:
            _ = queue.get_nowait()
        except asyncio.QueueEmpty:
            return False
        queue.task_done()
        self._queue_discards[symbol] = self._queue_discards.get(symbol, 0) + 1
        obs_metrics.QUEUE_DROPS.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "queue_discards",
            {"symbol": symbol, "count": self._queue_discards[symbol]},
        )
        return True

    def _reject_candle(self, symbol: str, candle: dict, reason: str) -> None:
        self._queue_discards[symbol] = self._queue_discards.get(symbol, 0) + 1
        obs_metrics.QUEUE_DROPS.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "queue_discards",
            {"symbol": symbol, "count": self._queue_discards[symbol]},
        )
        registrar_vela_rechazada(symbol, reason)
        payload = {
            "evento": "queue_reject",
            "symbol": symbol,
            "reason": reason,
            "policy": self._queue_policy_for(symbol),
            "timestamp": candle.get("timestamp"),
        }
        log.warning(json.dumps(payload, ensure_ascii=False))

    def _coalesce_with_tail(
        self,
        symbol: str,
        queue: asyncio.Queue,
        candle: dict,
    ) -> bool:
        if self._coalesce_window_ms <= 0:
            return False
        tail = None
        try:
            tail = queue._queue[-1]  # type: ignore[attr-defined]
        except (AttributeError, IndexError):
            return False
        last_ts = tail.get("timestamp")
        new_ts = candle.get("timestamp")
        if last_ts is None or new_ts is None:
            return False
        if new_ts < last_ts or new_ts - last_ts > self._coalesce_window_ms:
            return False
        tail_high = float(tail.get("high", candle.get("high", 0.0)))
        tail_low = float(tail.get("low", candle.get("low", 0.0)))
        tail_volume = float(tail.get("volume", 0.0))
        candle_high = float(candle.get("high", tail_high))
        candle_low = float(candle.get("low", tail_low))
        candle_volume = float(candle.get("volume", 0.0))
        tail["high"] = max(tail_high, candle_high)
        tail["low"] = min(tail_low, candle_low)
        tail["close"] = candle.get("close", tail.get("close"))
        tail["volume"] = tail_volume + candle_volume
        tail["_coalesced"] = tail.get("_coalesced", 0) + 1
        obs_metrics.QUEUE_COALESCE_TOTAL.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "queue_coalesce",
            {"symbol": symbol, "count": tail["_coalesced"]},
        )
        self._coalesce_counts[symbol] = self._coalesce_counts.get(symbol, 0) + 1
        payload = {
            "evento": "queue_coalesce",
            "symbol": symbol,
            "timestamp": new_ts,
            "coalesced_count": tail["_coalesced"],
        }
        log.info(json.dumps(payload, ensure_ascii=False))
        return True

    async def _handle_queue_full(
        self,
        symbol: str,
        queue: asyncio.Queue,
        candle: dict,
        policy: str,
    ) -> Literal["retry", "handled", "rejected", "inserted"]:
        policy = policy.lower()
        if policy == "drop_oldest":
            dropped = self._drop_oldest(symbol, queue)
            if dropped:
                registrar_vela_rechazada(symbol, "backpressure")
                return "retry"
            return "rejected"
        if policy == "coalesce":
            if self._coalesce_with_tail(symbol, queue, candle):
                return "handled"
            fallback = self._queue_safety_policy
            obs_metrics.QUEUE_POLICY_FALLBACK_TOTAL.labels(
                symbol=symbol, policy=fallback
            ).inc()
            if fallback == "drop_oldest":
                dropped = self._drop_oldest(symbol, queue)
                if dropped:
                    registrar_vela_rechazada(symbol, "coalesce_fallback")
                    return "retry"
                return "rejected"
            if fallback == "block":
                await queue.put(candle)
                return "inserted"
            self._reject_candle(symbol, candle, "coalesce_reject")
            return "rejected"
        if policy == "reject":
            self._reject_candle(symbol, candle, "queue_reject")
            return "rejected"
        # Por defecto, bloquear
        await queue.put(candle)
        return "inserted"

    async def _process_candle(
        self,
        symbol: str,
        candle: dict,
        handler: Callable[[dict], Awaitable[None]],
        queue: asyncio.Queue,
    ) -> None:
        """Procesa una única vela aplicando timeout y métricas."""
        enqueue_ts = candle.get("_df_enqueue_time")
        trace_id = candle.get("trace_id")
        queue_wait_ms: float | None = None
        if enqueue_ts is not None:
            queue_wait_ms = (time.monotonic() - enqueue_ts) * 1000
            candle["_df_queue_wait_ms"] = queue_wait_ms
            log.debug(
                f"[{symbol}] Handler esperó {queue_wait_ms/1000:.3f}s antes de iniciar "
                f"trace_id={trace_id}"
            )
        else:
            log.debug(
                f"[{symbol}] Handler sin timestamp de espera previo trace_id={trace_id}"
            )
        start = time.perf_counter()
        try:
            candle["_df_dequeue_time"] = time.monotonic()
            await asyncio.wait_for(handler(candle), timeout=self.handler_timeout)
        except asyncio.TimeoutError:
            self._handler_timeouts[symbol] = (
                self._handler_timeouts.get(symbol, 0) + 1
            )
            obs_metrics.HANDLER_TIMEOUTS.labels(symbol=symbol).inc()
            registro_metrico.registrar(
                "handler_timeouts",
                {"symbol": symbol, "count": self._handler_timeouts[symbol]},
            )
            log.error(
                f"Handler de {symbol} superó {self.handler_timeout}s; omitiendo vela "
                f"(total {self._handler_timeouts[symbol]})"
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"Error procesando vela en {symbol}: {e}")
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            queue.task_done()
            if enqueue_ts is not None:
                INGEST_LATENCY.labels(symbol=symbol).observe(
                    time.monotonic() - enqueue_ts
                )
            QUEUE_SIZE.labels(symbol=symbol).set(queue.qsize())
            ts = candle.get("timestamp")
            if ts is not None:
                age = max(0.0, time.time() - ts / 1000)
                obs_metrics.CANDLE_AGE.labels(symbol=symbol).observe(age)
            metrics = self._get_stage_metrics(symbol)
            metrics.record(
                latency=duration_ms,
                queue_delay=queue_wait_ms,
                queue_size=float(queue.qsize()),
                extra={"symbol": symbol, "trace_id": trace_id},
            )
            metrics.maybe_flush(log, extra={"symbol": symbol})
            self._consumer_last[symbol] = time.monotonic()
            if ts is not None:
                forced = bool(candle.get("_df_forced"))
                prev_ts = self._consumer_last_processed_ts.get(symbol)
                if prev_ts is not None and ts <= prev_ts and not forced:
                    qsize_actual = queue.qsize()
                    maxsize = getattr(queue, "maxsize", 0) or 0
                    self._handle_consumer_loop(
                        symbol, ts, prev_ts, qsize_actual, maxsize
                    )
                self._consumer_last_processed_ts[symbol] = ts
                obs_metrics.CONSUMER_LAST_TIMESTAMP.labels(symbol=symbol).set(
                    ts / 1000
                )

    def _schedule_backfill_retry(self, symbol: str) -> None:
        """Programa un reintento diferido para ``_backfill_candles`` controlando la cadencia."""

        existing = self._backfill_retry_tasks.get(symbol)
        current_task = asyncio.current_task()
        if (
            existing
            and not existing.done()
            and existing is not current_task
        ):
            log.debug(f"[{symbol}] Reintento de backfill ya programado")
            return

        delay = max(0.0, self._backfill_retry_delay)
        now = time.monotonic()
        last = self._backfill_retry_last.get(symbol, 0.0)
        if now - last < delay and (existing is None or existing.done()):
            log.debug(
                f"[{symbol}] Reintento de backfill omitido por throttle (delta={now - last:.3f}s)"
            )
            return

        async def _delayed_retry() -> None:
            try:
                await asyncio.sleep(delay)
                await self._backfill_candles(symbol)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(f"[{symbol}] Error en reintento diferido de backfill")
            finally:
                self._backfill_retry_tasks.pop(symbol, None)

        self._backfill_retry_last[symbol] = now
        task = asyncio.create_task(
            _delayed_retry(), name=f"datafeed-backfill-retry-{symbol}"
        )
        self._backfill_retry_tasks[symbol] = task

    @property
    def activos(self) -> list[str]:
        """Lista de símbolos con streams activos."""
        return list(self._symbols)
    
    @property
    def handler_timeouts(self) -> Dict[str, int]:
        """Contador de velas descartadas por exceder ``handler_timeout``."""
        return dict(self._handler_timeouts)
    
    @property
    def queue_discards(self) -> Dict[str, int]:
        """Cantidad de velas descartadas por cola llena por símbolo."""
        return dict(self._queue_discards)
    
    @property
    def coalesce_counts(self) -> Dict[str, int]:
        """Cantidad de velas colapsadas (coalescidas) por símbolo."""
        return dict(self._coalesce_counts)
    
    def set_reset_callback(self, cb: Callable[[str], None]) -> None:
        """Registra un callback para reiniciar el filtro de velas por símbolo."""
        self._reset_cb = cb

    def set_estado(self, estado: Dict[str, Any]) -> None:
        """Inyecta el estado del trader para validaciones."""
        self._estado = estado

    def verificar_continuidad(self) -> bool:
        """Comprueba que los buffers precargados no tengan gaps ni duplicados."""
        if self._estado is None:
            return True
        ok = True
        for symbol, est in self._estado.items():
            if not validar_integridad_velas(symbol, self.intervalo, est.buffer, log):
                ok = False
        return ok
    

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""

        async def wrapper(candle: dict) -> None:
            await self._handle_candle(symbol, candle)

        await self._relanzar_stream(symbol, wrapper)

    async def stream_combined(
        self, symbols: list[str], handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Escucha velas de varios símbolos usando un único WebSocket."""

        def make_wrapper(sym: str) -> Callable[[dict], Awaitable[None]]:
            async def _wrapper(candle: dict) -> None:
                await self._handle_candle(sym, candle)

            return _wrapper

        handlers = {sym: make_wrapper(sym) for sym in symbols}
        await self._relanzar_streams_combinado(symbols, handlers)

    def _should_enqueue_candle(self, symbol: str, candle: dict, force: bool = False) -> bool:
        """Valida que la vela esté cerrada y no se haya procesado ya."""
        if not candle.get('is_closed', True):
            log.debug(f'[{symbol}] Ignorando vela abierta: {candle}')
            return False
        ts = candle.get('timestamp')
        if ts is None:
            log.debug(f'[{symbol}] Vela sin timestamp: {candle}')
            return False
        if not timestamp_alineado(ts, self.intervalo):
            log.error(
                f'❌ Timestamp desalineado en vela de {symbol}: {ts}'
            )
            return False
        last = self._last_close_ts.get(symbol)
        if last is not None and ts <= last and not force:
            reason = "duplicada" if ts == last else "fuera_de_orden"
            log.debug(f'[{symbol}] Vela {reason} ignorada: {ts}')
            registrar_vela_rechazada(symbol, reason)
            return False
        self._last_close_ts[symbol] = ts
        return True
    
    async def _handle_candle(self, symbol: str, candle: dict, force: bool = False) -> None:
        """Valida y encola la vela actualizando heartbeats solo al éxito."""
        if not self._should_enqueue_candle(symbol, candle, force=force):
            return
        queue = self._queues[symbol]
        qsize = queue.qsize()
        maxsize = getattr(queue, "maxsize", 0) or 0
        if maxsize:
            self._emit_high_watermark(symbol, qsize, maxsize)
        candle.setdefault("trace_id", uuid4().hex)
        candle["_df_enqueue_time"] = time.monotonic()
        candle["_df_forced"] = bool(force)

        def _update_producer_stats() -> None:
            stats = self._producer_stats.get(symbol)
            now = time.monotonic()
            if stats is None:
                self._producer_stats[symbol] = {
                    "last": now,
                    "count": 0,
                    "last_rate": 0.0,
                }
                stats = self._producer_stats[symbol]
            stats["count"] += 1
            if now - stats["last"] >= 2.0:
                rate = stats["count"] / (now - stats["last"])
                obs_metrics.PRODUCER_RATE.labels(symbol=symbol).set(rate)
                stats["last_rate"] = rate
                stats["count"] = 0
                stats["last"] = now

        policy = self._queue_policy_for(symbol)
        inserted_or_handled = False
        if policy == "block":
            await queue.put(candle)
            inserted_or_handled = True
        else:
            while True:
                try:
                    queue.put_nowait(candle)
                    inserted_or_handled = True
                    break
                except asyncio.QueueFull:
                    action = await self._handle_queue_full(symbol, queue, candle, policy)
                    if action == "retry":
                        continue
                    if action == "inserted":
                        inserted_or_handled = True
                        break
                    if action == "handled":
                        inserted_or_handled = True
                        break
                    if action == "rejected":
                        return
        if inserted_or_handled:
            QUEUE_SIZE.labels(symbol=symbol).set(queue.qsize())
            _update_producer_stats()
        
        self._last[symbol] = datetime.now(UTC)
        self._last_monotonic[symbol] = time.monotonic()
        self._mensajes_recibidos[symbol] = (
            self._mensajes_recibidos.get(symbol, 0) + 1
        )
        self._ultimo_candle[symbol] = {
            'timestamp': candle.get('timestamp'),
            'open': candle.get('open'),
            'high': candle.get('high'),
            'low': candle.get('low'),
            'close': candle.get('close'),
            'volume': candle.get('volume'),
        }
        log.debug(
            f'[{symbol}] Recibida vela trace_id={candle.get("trace_id")}: '
            f'timestamp={candle.get("timestamp")}'
        )
        tick_data(symbol)

    def _update_consumer_state(
        self, symbol: str, state: ConsumerState, *, reason: str | None = None
    ) -> None:
        """Actualiza el estado interno y expone métricas/registro."""

        anterior = self._consumer_status.get(symbol)
        motivo_prev = self._consumer_state_reason.get(symbol)
        cambiado = anterior != state or motivo_prev != reason
        if reason:
            self._consumer_state_reason[symbol] = reason
        else:
            self._consumer_state_reason.pop(symbol, None)
        self._consumer_status[symbol] = state
        obs_metrics.CONSUMER_STATE.labels(symbol=symbol).set(float(state.value))
        if not cambiado:
            return
        payload = {"symbol": symbol, "state": state.name.lower()}
        if reason:
            payload["reason"] = reason
        registro_metrico.registrar("consumer_state", payload)
        msg = f"[{symbol}] consumer_state={state.name}"
        if reason:
            msg += f" ({reason})"
        if state is ConsumerState.HEALTHY:
            log.info(msg)
        elif state is ConsumerState.STALLED:
            log.error(msg)
        elif state is ConsumerState.LOOP:
            log.error(msg)
        else:
            log.info(msg)

    def _reset_consumer_counters(self, symbol: str) -> None:
        """Limpia ventanas móviles y resetea métricas internas."""

        _ = self._consumer_counter_locks[symbol]
        self._consumer_history[symbol] = deque()
        self._consumer_window_totals[symbol] = 0
        self._consumer_last_rate[symbol] = 0.0
        self._consumer_prev_qsize.pop(symbol, None)
        self._consumer_last_log.pop(symbol, None)
        obs_metrics.CONSUMER_RATE.labels(symbol=symbol).set(0.0)
        self._update_consumer_state(symbol, ConsumerState.STARTING)

    async def _register_consumer_progress(
        self, symbol: str, processed: int, timestamp: float
    ) -> float:
        """Actualiza la ventana deslizante de consumo y devuelve el nuevo rate."""

        async with self._consumer_counter_locks[symbol]:
            history = self._consumer_history[symbol]
            history.append((timestamp, processed))
            total = self._consumer_window_totals[symbol] + processed
            cutoff = timestamp - self._consumer_rate_window
            while history and history[0][0] < cutoff:
                _, old = history.popleft()
                total -= old
            self._consumer_window_totals[symbol] = total
            if history:
                span = max(timestamp - history[0][0], self._consumer_min_window_span)
            else:
                span = self._consumer_rate_window
            rate = total / span if span > 0 else 0.0
            self._consumer_last_rate[symbol] = rate
            return rate

    def _emit_consumer_metrics(
        self,
        symbol: str,
        consumer_rate: float,
        producer_rate: float,
        qsize: int,
        maxsize: int,
    ) -> None:
        """Registra métricas de consumo en logs y ``registro_metrico``."""

        state = self._consumer_status.get(symbol, ConsumerState.STARTING)
        capacity = f"{qsize}/{maxsize}" if maxsize else f"{qsize}"
        last_ts = self._consumer_last_processed_ts.get(symbol)
        msg = (
            f"[{symbol}] consumer_rate={consumer_rate:.2f}/s "
            f"producer_rate={producer_rate:.2f}/s qsize={capacity} "
            f"state={state.name}"
        )
        if last_ts is not None:
            msg += f" last_ts={last_ts}"
        logger = log.warning if qsize else log.info
        logger(msg)
        data = {
            "symbol": symbol,
            "consumer_rate": float(consumer_rate),
            "producer_rate": float(producer_rate),
            "qsize": int(qsize),
            "qmax": int(maxsize),
            "state": state.name.lower(),
        }
        motivo = self._consumer_state_reason.get(symbol)
        if motivo:
            data["state_reason"] = motivo
        if last_ts is not None:
            data["last_timestamp"] = int(last_ts)
        registro_metrico.registrar("consumer_metrics", data)

    def _handle_consumer_stall(
        self,
        symbol: str,
        rate: float,
        qsize: int,
        maxsize: int,
        producer_rate: float,
        now: float,
    ) -> None:
        """Marca el consumer como atascado y dispara reinicio controlado."""

        reason = f"consumer_rate≈0 qsize={qsize}"
        self._update_consumer_state(symbol, ConsumerState.STALLED, reason=reason)
        obs_metrics.CONSUMER_STALLS.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "consumer_stall",
            {
                "symbol": symbol,
                "qsize": int(qsize),
                "qmax": int(maxsize),
                "consumer_rate": float(rate),
                "producer_rate": float(producer_rate),
            },
        )
        self._consumer_last_log[symbol] = now
        self._emit_consumer_metrics(symbol, rate, producer_rate, qsize, maxsize)
        raise ConsumerStalledError(f"[{symbol}] consumer sin progreso: {reason}")

    def _check_consumer_health(
        self,
        symbol: str,
        rate: float,
        qsize: int,
        maxsize: int,
        producer_rate: float,
        now: float,
    ) -> None:
        """Evalúa el estado del consumer y aplica verificaciones de seguridad."""

        prev_qsize = self._consumer_prev_qsize.get(symbol)
        self._consumer_prev_qsize[symbol] = qsize
        if rate <= self._consumer_rate_epsilon:
            if prev_qsize is not None and qsize > prev_qsize:
                self._handle_consumer_stall(
                    symbol, rate, qsize, maxsize, producer_rate, now
                )
                return
        else:
            self._update_consumer_state(symbol, ConsumerState.HEALTHY)
        last_log = self._consumer_last_log.get(symbol, 0.0)
        if now - last_log >= self._metrics_log_interval:
            self._consumer_last_log[symbol] = now
            self._emit_consumer_metrics(symbol, rate, producer_rate, qsize, maxsize)

    def _handle_consumer_loop(
        self,
        symbol: str,
        current_ts: int,
        prev_ts: int,
        qsize: int,
        maxsize: int,
    ) -> None:
        """Detecta procesamiento repetido del mismo timestamp y reinicia."""

        reason = (
            f"timestamp no avanza prev={prev_ts} actual={current_ts} qsize={qsize}"
        )
        self._update_consumer_state(symbol, ConsumerState.LOOP, reason=reason)
        obs_metrics.CONSUMER_PROGRESS_FAILURES.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "consumer_progress_error",
            {
                "symbol": symbol,
                "prev_timestamp": int(prev_ts),
                "current_timestamp": int(current_ts),
                "qsize": int(qsize),
                "qmax": int(maxsize),
            },
        )
        producer_rate = self._producer_stats.get(symbol, {}).get("last_rate", 0.0)
        rate = self._consumer_last_rate.get(symbol, 0.0)
        self._consumer_last_log[symbol] = time.monotonic()
        self._emit_consumer_metrics(symbol, rate, producer_rate, qsize, maxsize)
        raise ConsumerProgressError(
            f"[{symbol}] consumer procesando sin avanzar timestamp"
        )

    async def _backfill_candles(self, symbol: str) -> None:
        """Recupera velas faltantes y evita llamadas repetidas."""
        if not self._cliente:
            return
        
        intervalo_ms = self.intervalo_segundos * 1000
        ahora = int(datetime.now(UTC).timestamp() * 1000)
        last_ts = self._last_close_ts.get(symbol)
        cached = self._last_backfill_ts.get(symbol)
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
        restante_total = min(max(faltan, self.min_buffer_candles), BACKFILL_MAX_CANDLES)
        queue = self._queues.get(symbol)
        if queue and queue.maxsize and queue.qsize() > queue.maxsize * 0.5:
            log.warning(
                f"[{symbol}] backfill diferido por presión de cola ({queue.qsize()}/{queue.maxsize})"
            )
            self._schedule_backfill_retry(symbol)
            return
        log.info(
            f"[{symbol}] Backfill solicitado faltantes={faltan} min_required={self.min_buffer_candles}"
        )
        ohlcv: list[list[float]] = []
        total_chunks = (restante_total + 99) // 100
        for idx in range(total_chunks):
            limite_chunk = min(100, restante_total)
            intento = 0
            while True:
                try:
                    chunk = await fetch_ohlcv_async(
                        self._cliente,
                        symbol,
                        self.intervalo,
                        since=since,
                        limit=limite_chunk,
                    )
                    break
                except (AuthenticationError, NetworkError) as e:
                    intento += 1
                    if intento >= 3:
                        log.warning(f'❌ Error obteniendo backfill para {symbol}: {e}')
                        return
                    await asyncio.sleep(0.5 * intento)
                except Exception:
                    log.exception(f'❌ Error inesperado obteniendo backfill para {symbol}')
                    return
            log.info(
                f'[{symbol}] Backfill chunk {idx + 1}/{total_chunks} con {len(chunk)} velas'
            )
            if not chunk:
                break
            ohlcv.extend(chunk)
            restante_total -= len(chunk)
            since = chunk[-1][0] + 1
            if len(chunk) < limite_chunk or restante_total <= 0:
                break
            # cede el control al loop entre chunks para evitar monopolizar
            await asyncio.sleep(0)
        if not ohlcv:
            return
        log.info(
            f"[{symbol}] Backfill completado rellenadas={len(ohlcv)} faltantes={faltan} min_required={self.min_buffer_candles}"
        )
        self._last_backfill_ts[symbol] = ohlcv[-1][0]
        validar_integridad_velas(
            symbol,
            self.intervalo,
            ({"timestamp": o[0]} for o in ohlcv),
            log,
        )
        registro_metrico.registrar(
            'velas_backfill',
            {'symbol': symbol, 'tf': self.intervalo, 'count': len(ohlcv)},
        )
        registro_metrico.registrar(
            'delta_backfill',
            {'symbol': symbol, 'tf': self.intervalo, 'delta': faltan},
        )
        for o in ohlcv:
            candle = {
                'symbol': symbol,
                'timestamp': o[0],
                'open': float(o[1]),
                'high': float(o[2]),
                'low': float(o[3]),
                'close': float(o[4]),
                'volume': float(o[5]),
                'is_closed': True,
            }
            prev = self._ultimo_candle.get(symbol)
            if prev and prev.get('timestamp') == candle['timestamp']:
                if any(prev.get(k) != candle[k] for k in ('open', 'high', 'low', 'close', 'volume')):
                    await self._handle_candle(symbol, candle, force=True)
            else:
                await self._handle_candle(symbol, candle)


    async def _consumer(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Procesa las velas encoladas para ``symbol`` de forma asíncrona."""
        queue = self._queues[symbol]
        self._queue_windows.setdefault(symbol, [])
        self._last_window_reset[symbol] = time.monotonic()
        self._reset_consumer_counters(symbol)
        while self._running:
            first = await queue.get()
            maxsize = getattr(queue, "maxsize", 0) or 0
            qsz = queue.qsize()
            if maxsize and qsz / maxsize >= 0.8:
                dropped = 0
                objetivo = int(maxsize * 0.5)
                while queue.qsize() > objetivo:
                    try:
                        _ = queue.get_nowait()
                        queue.task_done()
                        dropped += 1
                    except asyncio.QueueEmpty:
                        break
                if dropped:
                    self._queue_discards[symbol] = (
                        self._queue_discards.get(symbol, 0) + dropped
                    )
                    obs_metrics.QUEUE_DROPS.labels(symbol=symbol).inc(dropped)
                    registro_metrico.registrar(
                        "queue_discards",
                        {"symbol": symbol, "count": self._queue_discards[symbol]},
                    )
                    registrar_vela_rechazada(symbol, "backpressure")
                    if _should_log(f"backpressure:{symbol}", every=2.0):
                        log.warning(
                            f"[{symbol}] high-watermark drop {dropped} old candles (qsize={queue.qsize()}/{queue.maxsize})"
                        )
            candles = [first]
            for _ in range(self.batch_size - 1):
                if queue.empty():
                    break
                try:
                    candles.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if hasattr(asyncio, "TaskGroup"):
                async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
                    for c in candles:
                        tg.create_task(
                            self._process_candle(symbol, c, handler, queue)
                        )
            else:
                # Python 3.10 o menor: usamos gather
                await asyncio.gather(
                    *(self._process_candle(symbol, c, handler, queue) for c in candles)
                )
            ahora = time.monotonic()
            rate = await self._register_consumer_progress(symbol, len(candles), ahora)
            obs_metrics.CONSUMER_RATE.labels(symbol=symbol).set(rate)
            qsize = queue.qsize()
            self._queue_windows[symbol].append(qsize)
            if ahora - self._last_window_reset[symbol] >= 60:
                vals = self._queue_windows[symbol]
                minimo = min(vals) if vals else 0
                maximo = max(vals) if vals else 0
                promedio = (sum(vals) / len(vals)) if vals else 0.0
                obs_metrics.QUEUE_SIZE_MIN.labels(symbol=symbol).set(minimo)
                obs_metrics.QUEUE_SIZE_MAX.labels(symbol=symbol).set(maximo)
                obs_metrics.QUEUE_SIZE_AVG.labels(symbol=symbol).set(promedio)
                self._queue_windows[symbol].clear()
                self._last_window_reset[symbol] = ahora
            producer_rate = self._producer_stats.get(symbol, {}).get("last_rate", 0.0)
            self._check_consumer_health(
                symbol,
                rate,
                qsize,
                maxsize,
                float(producer_rate or 0.0),
                ahora,
            )
                        
    async def _relanzar_stream(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Mantiene un loop de conexión para ``symbol`` con backoff exponencial."""
        fallos_consecutivos = 0
        primera_vez = True
        while True:
            try:
                if primera_vez:
                    await asyncio.sleep(random.random())
                    primera_vez = False
                beat(f'stream_{symbol}', 'backfill_start')
                await asyncio.wait_for(
                    self._backfill_candles(symbol), timeout=self.tiempo_inactividad
                )
                beat(f'stream_{symbol}', 'backfill_end')
                beat(f'stream_{symbol}', 'listen_start')
                # El propio WebSocket gestiona los timeouts de inactividad via
                # ``_watchdog``; evitar un ``wait_for`` externo mantiene una única
                # fuente de verdad y simplifica el flujo de errores.
                await escuchar_velas(
                    symbol,
                    self.intervalo,
                    handler,
                    self._last,
                    self.tiempo_inactividad,
                    self.ping_interval,
                    cliente=self._cliente,
                    mensaje_timeout=self.tiempo_inactividad,
                    backpressure=self.backpressure,
                    ultimo_timestamp=self._last_close_ts.get(symbol),
                    ultimo_cierre=(self._ultimo_candle.get(symbol, {}).get('close')
                                   if self._ultimo_candle.get(symbol) else None),
                )
                beat(f'stream_{symbol}', 'listen_end')
                delay = calcular_backoff(fallos_consecutivos)
                log.info(
                    f'🔁 Conexión de {symbol} finalizada; reintentando en {delay:.1f}s'
                )
                fallos_consecutivos = 0
                await asyncio.sleep(delay)
            except asyncio.TimeoutError:
                beat(f'stream_{symbol}', 'timeout')
                fallos_consecutivos += 1
                delay = calcular_backoff(fallos_consecutivos)
                msg = (
                    f'⌛ Timeout en stream {symbol}; reintentando en {delay:.1f}s'
                )
                if fallos_consecutivos > 2:
                    log.warning(msg)
                else:
                    log.info(msg)
                await asyncio.sleep(delay)
                continue
            except asyncio.CancelledError:
                beat(f'stream_{symbol}', 'cancel')
                raise
            except InactividadTimeoutError:
                beat(f'stream_{symbol}', 'inactivity')
                delay = calcular_backoff(fallos_consecutivos)
                log.warning(
                    f'🔕 Stream {symbol} reiniciado por inactividad; reintentando en {delay:.1f}s'
                )
                fallos_consecutivos = 0
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                beat(f'stream_{symbol}', 'error')
                fallos_consecutivos += 1
                try:
                    if (
                        self.notificador is not None
                        and (fallos_consecutivos == 1 or fallos_consecutivos % 5 == 0)
                    ):
                        await self.notificador.enviar_async(
                            f'🔄 Stream {symbol} en reconexión (intento {fallos_consecutivos})',
                            'WARN',
                        )
                except Exception:
                    log.exception('Error enviando notificación de reconexión')
                    tick('data_feed')
                    
                tick('data_feed')
                if isinstance(e, AuthenticationError) and (
                    getattr(e, 'code', None) == -2015 or '-2015' in str(e)
                ):
                    beat(f'stream_{symbol}', 'auth')
                    task_cooldown[f'stream_{symbol}'] = datetime.now(UTC) + timedelta(minutes=10)
                    log.error(f'❌ Auth fallida para {symbol}; esperando 600s')
                    await asyncio.sleep(600)
                    continue

                delay = calcular_backoff(fallos_consecutivos)
                if isinstance(e, (AuthenticationError, NetworkError)):
                    log.warning(
                        f'⚠️ Stream {symbol} falló: {e}. Reintentando en {delay:.1f}s'
                    )
                else:
                    log.exception(
                        f'⚠️ Stream {symbol} falló de forma inesperada. Reintentando en {delay:.1f}s'
                    )
                    
                if fallos_consecutivos >= self.max_stream_restarts:
                    log.error(
                        f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos'
                    )
                    log.debug(
                        f"Stream {symbol} detenido tras {fallos_consecutivos} intentos"
                    )
                    try:
                        if self.notificador is not None:
                            await self.notificador.enviar_async(
                                f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos',
                                'CRITICAL',
                            )
                    except Exception:
                        log.exception('Error enviando notificación de límite de reconexiones')
                        tick('data_feed')
                    raise
                await asyncio.sleep(delay)

    
    async def _relanzar_streams_combinado(
        self, symbols: list[str], handlers: Dict[str, Callable[[dict], Awaitable[None]]]
    ) -> None:
        """Mantiene un loop de conexión combinado para varios símbolos."""
        fallos_consecutivos = 0
        primera_vez = True
        while True:
            try:
                if primera_vez:
                    await asyncio.sleep(random.random())
                    primera_vez = False
                for sym in symbols:
                    beat(f'stream_{sym}', 'backfill_start')
                await asyncio.gather(
                    *[
                        asyncio.wait_for(
                            self._backfill_candles(sym),
                            timeout=self.tiempo_inactividad,
                        )
                        for sym in symbols
                    ]
                )
                for sym in symbols:
                    beat(f'stream_{sym}', 'backfill_end')
                    beat(f'stream_{sym}', 'listen_start')
                await escuchar_velas_combinado(
                    symbols,
                    self.intervalo,
                    handlers,
                    self._last,
                    self.tiempo_inactividad,
                    self.ping_interval,
                    cliente=self._cliente,
                    mensaje_timeout=self.tiempo_inactividad,
                    backpressure=self.backpressure,
                    ultimos={
                        s: {
                            'ultimo_timestamp': self._last_close_ts.get(s),
                            'ultimo_cierre': (
                                self._ultimo_candle.get(s, {}).get('close')
                                if self._ultimo_candle.get(s)
                                else None
                            ),
                        }
                        for s in symbols
                    },
                )
                for sym in symbols:
                    beat(f'stream_{sym}', 'listen_end')
                delay = calcular_backoff(fallos_consecutivos)
                log.info(
                    f'🔁 Conexión combinada finalizada; reintentando en {delay:.1f}s'
                )
                fallos_consecutivos = 0
                await asyncio.sleep(delay)
            except asyncio.TimeoutError:
                for sym in symbols:
                    beat(f'stream_{sym}', 'timeout')
                fallos_consecutivos += 1
                delay = calcular_backoff(fallos_consecutivos)
                msg = (
                    f'⌛ Timeout en stream combinado; reintentando en {delay:.1f}s'
                )
                if fallos_consecutivos > 2:
                    log.warning(msg)
                else:
                    log.info(msg)
                await asyncio.sleep(delay)
                continue
            except asyncio.CancelledError:
                for sym in symbols:
                    beat(f'stream_{sym}', 'cancel')
                raise
            except InactividadTimeoutError:
                for sym in symbols:
                    beat(f'stream_{sym}', 'inactivity')
                fallos_consecutivos = 0
                delay = calcular_backoff(fallos_consecutivos)  # definir antes de usar
                log.warning(
                    f'🔕 Stream combinado reiniciado por inactividad; reintentando en {delay:.1f}s'
                )
                await asyncio.sleep(delay)
                continue
            except Exception as e:
                for sym in symbols:
                    beat(f'stream_{sym}', 'error')
                fallos_consecutivos += 1
                try:
                    if (
                        self.notificador is not None
                        and (fallos_consecutivos == 1 or fallos_consecutivos % 5 == 0)
                    ):
                        await self.notificador.enviar_async(
                            f'🔄 Stream combinado en reconexión (intento {fallos_consecutivos})',
                            'WARN',
                        )
                except Exception:
                    log.exception('Error enviando notificación de reconexión')
                    tick('data_feed')
                tick('data_feed')
                if isinstance(e, AuthenticationError) and (
                    getattr(e, 'code', None) == -2015 or '-2015' in str(e)
                ):
                    for sym in symbols:
                        beat(f'stream_{sym}', 'auth')
                    task_cooldown['stream_combinado'] = datetime.now(UTC) + timedelta(
                        minutes=10
                    )
                    log.error(
                        '❌ Auth fallida para stream combinado; esperando 600s'
                    )
                    await asyncio.sleep(600)
                    continue
                delay = calcular_backoff(fallos_consecutivos)
                if isinstance(e, (AuthenticationError, NetworkError)):
                    log.warning(
                        f'⚠️ Stream combinado falló: {e}. Reintentando en {delay:.1f}s'
                    )
                else:
                    log.exception(
                        f'⚠️ Stream combinado falló de forma inesperada. Reintentando en {delay:.1f}s'
                    )
                if fallos_consecutivos >= self.max_stream_restarts:
                    log.error(
                        f'❌ Stream combinado superó el límite de {self.max_stream_restarts} intentos'
                    )
                    try:
                        if self.notificador is not None:
                            await self.notificador.enviar_async(
                                f'❌ Stream combinado superó el límite de {self.max_stream_restarts} intentos',
                                'CRITICAL',
                            )
                    except Exception:
                        log.exception(
                            'Error enviando notificación de límite de reconexiones'
                        )
                        tick('data_feed')
                    raise
                await asyncio.sleep(delay)


    async def iniciar(self) -> None:
        """Inicia el DataFeed usando la configuración almacenada."""
        if not self._symbols or not self._handler_actual:
            log.warning(
                "No se puede iniciar DataFeed: faltan símbolos o handler previo"
            )
            return
        await self.escuchar(self._symbols, self._handler_actual, self._cliente)

    
    async def _reconectar_por_supervisor(self, symbol: str) -> None:
        """Reinicia únicamente el stream afectado manteniendo el estado."""
        if not self._running:
            return
        async with self._locks[symbol]:
            if symbol in self._reiniciando:
                return
            self._reiniciando.add(symbol)
            log.critical(
                "🔁 Reinicio de DataFeed solicitado por supervisor (%s)", symbol
            )
            self.reinicios_forzados_total += 1
            try:
                await self._reiniciar_stream(symbol)
            finally:
                self._reiniciando.discard(symbol)


    async def _reiniciar_stream(self, symbol: str) -> None:
        """Reinicia el stream de ``symbol`` tras problemas en la cola."""
        if self._combined:
            task = next(iter(self._tasks.values()), None)
            if not task or not self._running:
                return
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=self.cancel_timeout)
                except asyncio.TimeoutError:
                    log.warning("⏱️ Timeout cancelando stream combinado")
                except Exception:
                    log.exception('Error cancelando stream combinado')
            nuevo = supervised_task(
                lambda: self.stream_combined(self._symbols, self._handler_actual),
                "stream_combinado",
                max_restarts=0,
            )
            for sym in self._symbols:
                self._tasks[sym] = nuevo
                tick_data(sym, reinicio=True)
                if self._reset_cb:
                    self._reset_cb(sym)
            log.info("📡 stream combinado reiniciado por timeout de cola")
            return
        task = self._tasks.get(symbol)
        if not task or not self._running:
            return
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=self.cancel_timeout)
            except asyncio.TimeoutError:
                log.warning(f"⏱️ Timeout cancelando stream {symbol}")
            except Exception:
                log.exception(f'Error cancelando stream {symbol}')
        self._tasks[symbol] = supervised_task(
            lambda sym=symbol: self.stream(sym, self._handler_actual),
            f"stream_{symbol}",
            max_restarts=0,
        )
        log.info(f"📡 stream reiniciado para {symbol} por timeout de cola")
        tick_data(symbol, reinicio=True)
        if self._reset_cb:
            self._reset_cb(symbol)
                

    async def _monitor_global_inactividad(self) -> None:
        """Vigila la actividad de los streams y los reinicia cuando es necesario."""
        try:
            while True:
                tick('data_feed')
                await asyncio.sleep(self.monitor_interval)
                if not self._running:
                    break
                if not self._tasks:
                    continue
                ahora = time.monotonic()
                if self._combined:
                    task = next(iter(self._tasks.values()), None)
                    inactivo = any(
                        (self._last_monotonic.get(sym) is not None)
                        and (ahora - self._last_monotonic.get(sym)) > self.tiempo_inactividad
                        for sym in self._symbols
                    )
                    if task and (task.done() or inactivo):
                        locks = [self._locks[s] for s in self._symbols]
                        for l in locks:
                            await l.acquire()
                        try:
                            if any(sym in self._reiniciando for sym in self._symbols):
                                continue
                            self._reiniciando.update(self._symbols)
                            log.warning(
                                "🔄 Stream combinado inactivo o finalizado; relanzando"
                            )
                            await self._reiniciar_stream(self._symbols[0])
                            if inactivo:
                                for sym in self._symbols:
                                    registrar_reinicio_inactividad(sym)
                                    self._reinicios_inactividad[sym] = (
                                        self._reinicios_inactividad.get(sym, 0) + 1
                                    )
                        finally:
                            for sym in self._symbols:
                                self._reiniciando.discard(sym)
                            for l in locks:
                                l.release()
                        continue

                for sym, task in list(self._tasks.items()):
                    ultimo = self._last_monotonic.get(sym)
                    inactivo = (
                        ultimo is not None
                        and (ahora - ultimo) > self.tiempo_inactividad
                    )
                    if task.done() or inactivo:
                        async with self._locks[sym]:
                            if sym in self._reiniciando:
                                continue
                            self._reiniciando.add(sym)
                            try:
                                log.warning(
                                    f"🔄 Stream {sym} inactivo o finalizado; relanzando",
                                )
                                stats = self._stream_restart_stats.setdefault(sym, deque())
                                ts_now = time.time()
                                stats.append(ts_now)
                                while stats and ts_now - stats[0] > 300:
                                    stats.popleft()
                                if (
                                    len(stats) > self.max_stream_restarts
                                    and ts_now - stats[0] < 60
                                ):
                                    log.error(
                                        f"🚨 Circuit breaker: demasiados reinicios para stream {sym}"
                                    )
                                    try:
                                        if self.notificador is not None:
                                            await self.notificador.enviar_async(
                                                f"🚨 Circuit breaker: demasiados reinicios para stream {sym}",
                                                "CRITICAL",
                                            )
                                    except Exception:
                                        log.exception(
                                            "Error enviando notificación de circuit breaker"
                                        )
                                        tick("data_feed")
                                    cons = self._consumer_tasks.get(sym)
                                    if cons and not cons.done():
                                        cons.cancel()
                                    self._consumer_tasks.pop(sym, None)
                                    self._queues.pop(sym, None)
                                    self._tasks.pop(sym, None)
                                    continue
                                backoff = min(60, 2 ** (len(stats) - 1))
                                await asyncio.sleep(backoff)
                                await self._reiniciar_stream(sym)
                                if inactivo:
                                    registrar_reinicio_inactividad(sym)
                                    self._reinicios_inactividad[sym] = (
                                        self._reinicios_inactividad.get(sym, 0) + 1
                                    )
                            finally:
                                self._reiniciando.discard(sym)
        
        except asyncio.CancelledError:
            tick('data_feed')
            raise
        except Exception:
            log.exception("Error inesperado en _monitor_global_inactividad")


    async def _monitor_consumers(self, timeout: int = 30) -> None:
        """Reinicia consumidores que no procesan velas en ``timeout`` segundos."""
        try:
            while True:
                await asyncio.sleep(5)
                if not self._running:
                    break
                ahora = time.monotonic()
                for sym in list(self._symbols):
                    ultimo = self._consumer_last.get(sym)
                    if ultimo is None:
                        continue
                    if ahora - ultimo > timeout:
                        registrar_watchdog_restart(f"consumer_{sym}")
                        if _should_log(f"consumer_watchdog:{sym}", every=2.0):
                            log.warning(
                                f"[{sym}] consumer sin progreso; reiniciando…"
                            )
                        task = self._consumer_tasks.get(sym)
                        if task and not task.done():
                            task.cancel()
                            try:
                                await asyncio.wait_for(task, timeout=self.cancel_timeout)
                            except Exception:
                                log.exception(
                                    f"Error cancelando consumer {sym}"
                                )
                        self._consumer_tasks[sym] = supervised_task(
                            lambda sym=sym: self._consumer(sym, self._handler_actual),
                            f"consumer_{sym}",
                            max_restarts=0,
                        )
                        self._consumer_last[sym] = ahora
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Error en monitor_consumers")
            tick('data_feed')

    async def escuchar(
        self,
        symbols: Iterable[str],
        handler: Callable[[dict], Awaitable[None]],
        cliente: Any | None = None,
        ultimos: dict[str, dict] | None = None,
    ) -> None:
        """Inicia un stream independiente por símbolo y espera a que finalicen.

        Si ``cliente`` se proporciona, se usará para recuperar velas perdidas tras
        una reconexión.
        """
        symbols_list = list(symbols)
        log.info("🎯 Símbolos recibidos: %s", symbols_list)
        await self.detener()
        self._handler_actual = handler
        self._symbols = symbols_list
        self._combined = len(self._symbols) > 1
        if cliente is not None:
            self._cliente = cliente
        if ultimos:
            for sym, data in ultimos.items():
                ts = data.get('timestamp')
                candle = data.get('candle')
                if ts is not None:
                    self._last_close_ts[sym] = ts
                if candle:
                    self._ultimo_candle[sym] = candle
        if self._estado:
            for sym in symbols_list:
                est = self._estado.get(sym)
                if not est or not getattr(est, "buffer", None):
                    continue
                warmup = [(c["timestamp"], c) for c in est.buffer if "timestamp" in c]
                last_ts = self._last_close_ts.get(sym)
                last_candle = self._ultimo_candle.get(sym)
                try:
                    merged = splice_with_last(warmup, last_ts, last_candle, self.intervalo)
                except ValueError as exc:
                    log.error(f"[{sym}] Warmup inconsistente: {exc}")
                    raise
                est.buffer.clear()
                est.buffer.extend(c for _, c in merged)
                if merged:
                    self._last_close_ts[sym] = merged[-1][0]
        self._running = True
        self._queues = {
            sym: asyncio.Queue(maxsize=self._queue_maxsize_for(sym))
            for sym in self._symbols
        }
        for sym in self._symbols:
            maxsize = getattr(self._queues[sym], "maxsize", 0) or 0
            log.debug(
                "[%s] Cola inicializada max=%s policy=%s",
                sym,
                maxsize,
                self._queue_policy_for(sym),
            )
            self._consumer_tasks[sym] = supervised_task(
                lambda sym=sym: self._consumer(sym, handler),
                f"consumer_{sym}",
                max_restarts=0,
            )
        if (
            self._monitor_consumers_task is None
            or self._monitor_consumers_task.done()
        ):
            self._monitor_consumers_task = asyncio.create_task(
                self._monitor_consumers()
            )
        if (
            self._monitor_global_task is None
            or self._monitor_global_task.done()
        ):
            self._monitor_global_task = asyncio.create_task(
                self._monitor_global_inactividad()
            )
        if self._combined:
            tarea = supervised_task(
                lambda: self.stream_combined(self._symbols, handler),
                "stream_combinado",
                max_restarts=0,
            )
            for sym in self._symbols:
                self._tasks[sym] = tarea
        else:
            for sym in self._symbols:
                if sym in self._tasks:
                    log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                    continue
                self._tasks[sym] = supervised_task(
                    lambda sym=sym: self.stream(sym, handler),
                    f'stream_{sym}',
                    max_restarts=0,
                )
        if self._tasks:
            while self._running and any(
                not t.done() for t in self._tasks.values()
            ):
                await asyncio.sleep(0.1)
        for nombre, tarea in self._tasks.items():
            estado = 'done' if tarea.done() else 'pending'
            if tarea.done() and tarea.exception():
                log.debug(
                    f"Tarea {nombre} finalizó con excepción: {tarea.exception()}"
                )
            else:
                log.debug(f"Tarea {nombre} estado: {estado}")

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        tasks: set[asyncio.Task] = set()
        if self._monitor_global_task:
            tasks.add(self._monitor_global_task)
        if self._monitor_consumers_task:
            tasks.add(self._monitor_consumers_task)
        tasks.update(self._tasks.values())
        tasks.update(self._consumer_tasks.values())

        for task in tasks:
            task.cancel()

        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning("🧟 Timeout cancelando tareas (tarea zombie)")
            except Exception:
                log.exception('Error cancelando tareas')
            tasks.clear()

        self._monitor_global_task = None
        self._monitor_consumers_task = None
        self._consumer_last.clear()
        self._tasks.clear()
        for sym in set(
            list(self._mensajes_recibidos.keys())
            + list(self._handler_timeouts.keys())
            + list(self._queue_discards.keys())
            + list(self._reinicios_inactividad.keys())
        ):
            log.info(
                "📊 %s: mensajes=%d, timeouts=%d, descartes=%d, reinicios_inactividad=%d",
                sym,
                self._mensajes_recibidos.get(sym, 0),
                self._handler_timeouts.get(sym, 0),
                self._queue_discards.get(sym, 0),
                self._reinicios_inactividad.get(sym, 0),
            )
        self._consumer_tasks.clear()
        self._queues.clear()
        self._last.clear()
        self._last_monotonic.clear()
        self._mensajes_recibidos.clear()
        self._queue_discards.clear()
        self._reinicios_inactividad.clear()
        for sym in list(self._consumer_status.keys()):
            obs_metrics.CONSUMER_STATE.labels(symbol=sym).set(
                float(ConsumerState.STARTING.value)
            )
        for sym in list(self._consumer_last_processed_ts.keys()):
            obs_metrics.CONSUMER_LAST_TIMESTAMP.labels(symbol=sym).set(0.0)
        self._consumer_history.clear()
        self._consumer_window_totals.clear()
        self._consumer_last_rate.clear()
        self._consumer_last_log.clear()
        self._consumer_prev_qsize.clear()
        self._consumer_status.clear()
        self._consumer_state_reason.clear()
        self._consumer_last_processed_ts.clear()
        for sym, metrics in self._stage_metrics.items():
            metrics.flush(log, extra={"symbol": sym})
        self._stage_metrics.clear()
        self._queue_alert_last.clear()
        self._symbols = []
        self._running = False
        self._combined = False
