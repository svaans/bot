"""
Versión simplificada de DataFeed para limpiar complejidad innecesaria.
- Enfoque: mínimo viable y estable para ingesta de velas cerradas.
- Mantiene: backfill básico, cola por símbolo con backpressure (drop_oldest|block),
  timeout del handler, reinicio por inactividad, opción combinado vs individual sin duplicaciones.
- Elimina: coalesce en cola, políticas múltiples por símbolo, exceso de métricas y contadores,
  notificador complejo (se reemplaza por callbacks), contadores poco accionables.

Dependencias esperadas (del proyecto original):
- binance_api.websocket: escuchar_velas, escuchar_velas_combinado, InactividadTimeoutError
- binance_api.cliente: fetch_ohlcv_async
- core.utils: intervalo_a_segundos, validar_integridad_velas, timestamp_alineado
- core.utils.logger: configurar_logger
- config.config: BACKFILL_MAX_CANDLES

API principal:
- DataFeedLite(intervalo, *, handler_timeout=2.0, inactivity_intervals=10,
               queue_max=2000, queue_policy="drop_oldest", monitor_interval=5,
               backpressure=True, cancel_timeout=5,
               on_event: callable | None = None)
- escuchar(symbols, handler, cliente=None)
- iniciar()  (relanza con la última config)
- detener()

Eventos (si se pasa on_event): on_event(evento: str, data: dict) -> None
"""
from __future__ import annotations
import asyncio
import os
import random
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List
from datetime import datetime, timezone, timedelta
from enum import Enum
from collections import defaultdict

from binance_api.websocket import (
    escuchar_velas,
    escuchar_velas_combinado,
    InactividadTimeoutError,
)
from binance_api.cliente import fetch_ohlcv_async
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos, validar_integridad_velas, timestamp_alineado
try:
    from config.config import BACKFILL_MAX_CANDLES
except (ImportError, AttributeError):  # pragma: no cover - compatibilidad tests
    BACKFILL_MAX_CANDLES = int(os.getenv("BACKFILL_MAX_CANDLES", "1000"))

UTC = timezone.utc
log = configurar_logger("datafeed", modo_silencioso=False)


def _safe_float(value: Any, default: float) -> float:
    """Convierte ``value`` a ``float`` devolviendo ``default`` ante error."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ConsumerState(Enum):
    STARTING = 0
    HEALTHY = 1
    STALLED = 2
    LOOP = 3


class DataFeed:
    """DataFeed minimalista: simple, legible y estable."""

    def __init__(
        self,
        intervalo: str,
        *,
        handler_timeout: float = float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
        inactivity_intervals: int = 10,
        queue_max: int = int(os.getenv("DF_QUEUE_MAX", "2000")),
        queue_policy: str = os.getenv("DF_QUEUE_POLICY", "drop_oldest").lower(),
        monitor_interval: int = 5,
        consumer_timeout_intervals: float | None = None,
        consumer_timeout_min: float | None = None,
        backpressure: bool = True,
        cancel_timeout: float = 5.0,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> None:
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)
        self.handler_timeout = max(0.1, handler_timeout)
        self.inactivity_intervals = max(1, inactivity_intervals)
        self.tiempo_inactividad = self.intervalo_segundos * self.inactivity_intervals
        self.queue_max = max(0, queue_max)
        self.queue_policy = queue_policy if queue_policy in {"drop_oldest", "block"} else "drop_oldest"
        monitor_interval_value = _safe_float(monitor_interval, 5.0)
        self.monitor_interval = max(0.05, monitor_interval_value)
        if consumer_timeout_intervals is None:
            consumer_timeout_intervals = _safe_float(
                os.getenv("DF_CONSUMER_TIMEOUT_INTERVALS"),
                1.5,
            )
        consumer_timeout_intervals = _safe_float(consumer_timeout_intervals, 1.5)
        self.consumer_timeout_intervals = max(1.0, consumer_timeout_intervals)
        if consumer_timeout_min is None:
            consumer_timeout_min = _safe_float(os.getenv("DF_CONSUMER_TIMEOUT_MIN"), 30.0)
        consumer_timeout_min = _safe_float(consumer_timeout_min, 30.0)
        self.consumer_timeout_min = max(0.0, consumer_timeout_min)
        raw_consumer_timeout = self.intervalo_segundos * self.consumer_timeout_intervals
        self.consumer_timeout = max(
            self.consumer_timeout_min,
            min(self.tiempo_inactividad, raw_consumer_timeout),
        )
        self.backpressure = bool(backpressure)
        self.cancel_timeout = max(0.5, cancel_timeout)
        self.on_event = on_event

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

        self._last_close_ts: Dict[str, int] = {}
        self._ultimo_candle: Dict[str, dict] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._consumer_last: Dict[str, float] = {}
        self._consumer_state: Dict[str, ConsumerState] = {}

        # Monitores
        self._monitor_inactividad_task: asyncio.Task | None = None
        self._monitor_consumers_task: asyncio.Task | None = None

        # Backfill
        self.min_buffer_candles = int(os.getenv("MIN_BUFFER_CANDLES", "30"))
        self._last_backfill_ts: Dict[str, int] = {}

    # ---------------- API pública ----------------
    @property
    def activos(self) -> bool:
        """Indica si el feed tiene streams activos."""

        if not self._running:
            return False
        return any(not t.done() for t in self._tasks.values())

    def verificar_continuidad(self, *, max_gap_intervals: int | None = None) -> bool:
        """Verifica que los backfills previos no tengan huecos grandes."""

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
        """Realiza backfill manual para ``symbols`` sin iniciar el stream."""

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
            for symbol in symbols_list:
                self._symbols = symbols_list
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
        cliente: Any | None = None,
    ) -> None:
        """Inicia el stream por símbolo (o combinado si hay >1)."""
        await self.detener()
        self._symbols = list({s.upper() for s in symbols})
        if not self._symbols:
            log.warning("Sin símbolos para escuchar()")
            return
        self._handler = handler
        self._cliente = cliente
        self._combined = len(self._symbols) > 1
        self._running = True

        # Colas + consumers
        self._queues = {s: asyncio.Queue(maxsize=self.queue_max) for s in self._symbols}
        for s in self._symbols:
            self._set_consumer_state(s, ConsumerState.STARTING)
            self._consumer_tasks[s] = asyncio.create_task(self._consumer(s), name=f"consumer_{s}")

        # Monitores (inactividad + consumers)
        self._monitor_inactividad_task = asyncio.create_task(self._monitor_inactividad(), name="monitor_inactividad")
        self._monitor_consumers_task = asyncio.create_task(
            self._monitor_consumers(self.consumer_timeout),
            name="monitor_consumers",
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
        if not self._symbols or not self._handler:
            log.warning("No se puede iniciar(): faltan símbolos o handler")
            return
        await self.escuchar(self._symbols, self._handler, self._cliente)

    async def detener(self) -> None:
        """Cancela tareas y limpia estado."""
        tasks = set()
        tasks.update(self._tasks.values())
        tasks.update(self._consumer_tasks.values())
        if self._monitor_inactividad_task:
            tasks.add(self._monitor_inactividad_task)
        if self._monitor_consumers_task:
            tasks.add(self._monitor_consumers_task)
        for t in tasks:
            t.cancel()
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=self.cancel_timeout)
            except asyncio.TimeoutError:
                log.warning("Timeout cancelando tareas")
        self._running = False
        self._tasks.clear()
        self._consumer_tasks.clear()
        self._queues.clear()
        self._monitor_inactividad_task = None
        self._monitor_consumers_task = None
        self._consumer_last.clear()
        self._consumer_state.clear()

    # ---------------- Internos: producción ----------------
    async def _stream_simple(self, symbol: str) -> None:
        backoff = 0.5
        primera_vez = True
        while self._running and symbol in self._queues:
            try:
                if primera_vez:
                    await asyncio.sleep(random.random())
                    primera_vez = False
                await self._do_backfill(symbol)
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
                    ultimo_cierre=(self._ultimo_candle.get(symbol, {}).get("close") if self._ultimo_candle.get(symbol) else None),
                )
                log.info(f"{symbol}: stream finalizado; reintentando…")
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)
            except InactividadTimeoutError:
                log.warning(f"{symbol}: reinicio por inactividad")
                backoff = 0.5
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(f"{symbol}: error en stream; reintentando")
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
                            "ultimo_cierre": (self._ultimo_candle.get(s, {}).get("close") if self._ultimo_candle.get(s) else None),
                        }
                        for s in symbols
                    },
                )
                log.info("stream combinado finalizado; reintentando…")
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)
            except InactividadTimeoutError:
                log.warning("stream combinado: reinicio por inactividad")
                backoff = 0.5
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("stream combinado: error; reintentando")
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

    # ---------------- Internos: cola y consumo ----------------
    async def _handle_candle(self, symbol: str, candle: dict) -> None:
        # Filtra solo velas cerradas y timestamps válidos (alineados a intervalo)
        if not candle.get("is_closed", True):
            return
        ts = candle.get("timestamp")
        if ts is None or not timestamp_alineado(ts, self.intervalo):
            return
        last = self._last_close_ts.get(symbol)
        if last is not None and ts <= last:
            return

        q = self._queues.get(symbol)
        if q is None:
            return
        candle.setdefault("_df_enqueue_time", time.monotonic())
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
                    except asyncio.QueueEmpty:
                        # si no se pudo liberar, reintentar ciclo
                        await asyncio.sleep(0)
                        continue
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
        handler = self._handler
        if handler is None:
            return
        while self._running:
            c = await q.get()
            start = time.perf_counter()
            try:
                await asyncio.wait_for(handler(c), timeout=self.handler_timeout)
                self._set_consumer_state(symbol, ConsumerState.HEALTHY)
            except asyncio.TimeoutError:
                log.error(f"{symbol}: handler superó {self.handler_timeout}s; vela omitida")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(f"{symbol}: error procesando vela")
            finally:
                q.task_done()
                self._consumer_last[symbol] = time.monotonic()
                # seguridad: detectar loop de mismo timestamp
                ts = c.get("timestamp")
                prev = getattr(self, f"_last_processed_{symbol}", None)
                if ts is not None:
                    if prev is not None and ts <= prev:
                        self._set_consumer_state(symbol, ConsumerState.LOOP)
                        log.error(f"{symbol}: consumer sin avanzar timestamp (prev={prev} actual={ts})")
                        await self._reiniciar_consumer(symbol)
                        continue
                    setattr(self, f"_last_processed_{symbol}", ts)

    async def _reiniciar_consumer(self, symbol: str) -> None:
        t = self._consumer_tasks.get(symbol)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=self.cancel_timeout)
            except Exception:
                pass
        self._consumer_tasks[symbol] = asyncio.create_task(self._consumer(symbol), name=f"consumer_{symbol}")

    # ---------------- Internos: monitores ----------------
    async def _monitor_inactividad(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.monitor_interval)
                ahora = time.monotonic()
                for s, last in list(self._last_monotonic.items()):
                    if last is None:
                        continue
                    if ahora - last > self.tiempo_inactividad:
                        log.warning(f"{s}: stream inactivo; reiniciando…")
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
                        log.warning(f"{s}: consumer detenido; reiniciando…")
                        await self._reiniciar_consumer(s)
                        self._consumer_last[s] = time.monotonic()
                        continue
                    if ahora - last <= current_timeout:
                        continue
                    queue = self._queues.get(s)
                    if queue is None or queue.empty():
                        continue
                    self._set_consumer_state(s, ConsumerState.STALLED)
                    log.warning(f"{s}: consumer sin progreso; reiniciando…")
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
                try:
                    await asyncio.wait_for(task, timeout=self.cancel_timeout)
                except Exception:
                    pass
            tarea = asyncio.create_task(self._stream_combinado(self._symbols), name="stream_combinado")
            for s in self._symbols:
                self._tasks[s] = tarea
        else:
            t = self._tasks.get(symbol)
            if t and not t.done():
                t.cancel()
                try:
                    await asyncio.wait_for(t, timeout=self.cancel_timeout)
                except Exception:
                    pass
            self._tasks[symbol] = asyncio.create_task(self._stream_simple(symbol), name=f"stream_{symbol}")

    # ---------------- Internos: backfill ----------------
    async def _do_backfill(self, symbol: str) -> None:
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
        ohlcv: List[List[float]] = []
        while restante_total > 0:
            limit = min(100, restante_total)
            try:
                chunk = await fetch_ohlcv_async(self._cliente, symbol, self.intervalo, since=since, limit=limit)
            except Exception:
                log.exception(f"{symbol}: error backfill")
                return
            if not chunk:
                break
            ohlcv.extend(chunk)
            restante_total -= len(chunk)
            since = chunk[-1][0] + 1
            if len(chunk) < limit:
                break
            await asyncio.sleep(0)
        if not ohlcv:
            return
        self._last_backfill_ts[symbol] = ohlcv[-1][0]
        validar_integridad_velas(symbol, self.intervalo, ({"timestamp": o[0]} for o in ohlcv), log)
        for o in ohlcv:
            c = {
                "symbol": symbol,
                "timestamp": o[0],
                "open": float(o[1]),
                "high": float(o[2]),
                "low": float(o[3]),
                "close": float(o[4]),
                "volume": float(o[5]),
                "is_closed": True,
            }
            await self._handle_candle(symbol, c)

    # ---------------- Utilidades ----------------
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
                log.exception("on_event lanzó excepción")

