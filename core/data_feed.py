# core/data_feed.py
from __future__ import annotations

"""
DataFeed — Ingesta estable de velas cerradas con backfill y validación en streaming.

Compatibilidad esperada con el resto del bot:
- Atributos:
    .activos (propiedad booleana)
    ._managed_by_trader (flag opcional; el Trader puede marcarlo en tiempo de arranque)
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
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=self.cancel_timeout)

        self._running = False
        self._tasks.clear()
        self._consumer_tasks.clear()
        self._queues.clear()
        self._monitor_inactividad_task = None
        self._monitor_consumers_task = None
        self._consumer_last.clear()
        self._consumer_state.clear()

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
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

            except InactividadTimeoutError:
                log.warning("%s: reinicio por inactividad", symbol)
                self._emit("ws_inactividad", {"symbol": symbol})
                backoff = 0.5
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.exception("%s: error en stream; reintentando", symbol)
                self._emit("ws_error", {"symbol": symbol, "error": str(e)})
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
                backoff = min(60.0, backoff * 2)
                await asyncio.sleep(backoff)

            except InactividadTimeoutError:
                log.warning("stream combinado: reinicio por inactividad")
                self._emit("ws_inactividad", {"symbols": symbols})
                backoff = 0.5
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.exception("stream combinado: error; reintentando")
                self._emit("ws_error", {"symbols": symbols, "error": str(e)})
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

        # Validación rápida de integridad (streaming) usando la propia vela
        # Espera dicts con al menos 'timestamp' y, si están presentes OHLCV, se validan.
        if not validar_integridad_velas(symbol, self.intervalo, [candle], log):
            self._emit("candle_invalid", {"symbol": symbol, "ts": ts})
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
        handler = self._handler
        if handler is None:
            return

        while self._running:
            c = await q.get()
            try:
                await asyncio.wait_for(handler(c), timeout=self.handler_timeout)
                self._set_consumer_state(symbol, ConsumerState.HEALTHY)
                self._stats[symbol]["processed"] += 1

            except asyncio.TimeoutError:
                log.error("%s: handler superó %.3fs; vela omitida", symbol, self.handler_timeout)
                self._emit("handler_timeout", {"symbol": symbol})
                self._stats[symbol]["handler_timeouts"] += 1

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.exception("%s: error procesando vela", symbol)
                self._emit("consumer_error", {"symbol": symbol, "error": str(e)})
                self._stats[symbol]["consumer_errors"] += 1

            finally:
                q.task_done()
                self._consumer_last[symbol] = time.monotonic()
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
                    if queue is None or queue.empty():
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



