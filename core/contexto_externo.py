"""Gestión de contexto fundamental recibido por streaming desde Binance."""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
import ssl
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from math import isclose
from typing import Awaitable, Callable, DefaultDict, Deque, Dict, Iterable

import websockets

# ❌ Eliminado: keepalive manual para evitar doble ping/pong
# from binance_api.websocket import _keepalive

from binance_api.cliente import BinanceClient, fetch_ohlcv_async, obtener_cliente
from core.utils.utils import configurar_logger, intervalo_a_segundos
from core.supervisor import supervised_task, tick
from core.utils.backoff import calcular_backoff
from core.registro_metrico import registro_metrico
from core.contexto_storage import PuntajeStore

try:  # pragma: no cover - métricas opcionales
    from observability.metrics import (
        CONTEXT_LAST_UPDATE_SECONDS,
        CONTEXT_PARSING_ERRORS_TOTAL,
        CONTEXT_SCORE_DISTRIBUTION,
        CONTEXT_UPDATE_LATENCY_SECONDS,
        CONTEXT_VOLUME_EXTREME_TOTAL,
    )
except Exception:  # pragma: no cover - fallback cuando prometheus no está disponible
    class _NullMetric:
        def labels(self, *_args, **_kwargs):  # type: ignore[override]
            return self

        def set(self, *_args, **_kwargs) -> None:
            return None

        def observe(self, *_args, **_kwargs) -> None:
            return None

        def inc(self, *_args, **_kwargs) -> None:
            return None

    CONTEXT_LAST_UPDATE_SECONDS = _NullMetric()  # type: ignore[assignment]
    CONTEXT_SCORE_DISTRIBUTION = _NullMetric()  # type: ignore[assignment]
    CONTEXT_UPDATE_LATENCY_SECONDS = _NullMetric()  # type: ignore[assignment]
    CONTEXT_PARSING_ERRORS_TOTAL = _NullMetric()  # type: ignore[assignment]
    CONTEXT_VOLUME_EXTREME_TOTAL = _NullMetric()  # type: ignore[assignment]

UTC = timezone.utc

log = configurar_logger('contexto_externo')
_PUNTAJES: Dict[str, float] = {}
_DATOS_EXTERNOS: Dict[str, dict] = {}
CONTEXT_WS_URL = 'wss://stream.binance.com:9443/ws/{symbol}@kline_5m'


def obtener_puntaje_contexto(symbol: str) -> float:
    """Devuelve el último puntaje conocido para ``symbol``."""
    valor = _PUNTAJES.get(symbol)
    try:
        return float(valor) if valor is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def obtener_todos_puntajes() -> dict:
    """Devuelve todos los puntajes actuales almacenados."""
    return dict(_PUNTAJES)


def obtener_datos_externos(symbol: str) -> dict:
    """Devuelve los últimos datos externos conocidos para ``symbol``."""
    return dict(_DATOS_EXTERNOS.get(symbol, {}))


class StreamContexto:
    """Conecta con Binance y actualiza el contexto en tiempo real."""

    def __init__(
        self,
        url_template: str | None = None,
        monitor_interval: int = 30,
        inactivity_timeout: int = 300,
        cancel_timeout: float = 5,
        # ⬆️ Hicimos más generoso el open_timeout para reducir "timed out during opening handshake"
        open_timeout: int = 60,
        connection_delay: float = 1.0,
        ping_interval: int = 20,
        ping_timeout: int = 10,
        max_retries: int = 5,
        backoff_base: float = 1.5,
        backoff_cap: float = 60.0,
        circuit_breaker: float = 300.0,
        ssl_context: ssl.SSLContext | bool | None = None,
        *,
        rest_poll_interval: int = 120,
        rest_timeframe: str = "5m",
        rest_freeze_tolerance: int | None = None,
        rest_client: BinanceClient | None = None,
        puntajes_store: PuntajeStore | None = None,
        puntajes_ttl: int | None = None,
        volume_window: int = 50,
        volume_baseline_min: int = 5,
        volume_extreme_multiplier: float = 8.0,
        score_window: int = 240,
        score_baseline_min: int = 20,
    ) -> None:
        self.url_template = url_template or CONTEXT_WS_URL
        self.monitor_interval = max(1, monitor_interval)
        self.inactivity_timeout = inactivity_timeout
        self.cancel_timeout = cancel_timeout
        self.open_timeout = open_timeout
        self.connection_delay = connection_delay
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.circuit_breaker = circuit_breaker
        self.ssl_context = ssl_context
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._last_event_ts: Dict[str, int] = {}
        self._monitor_task: asyncio.Task | None = None
        self._rest_task: asyncio.Task | None = None
        self._handler_actual: Callable[[str, float], Awaitable[None]] | None = None
        self._running = False
        self._symbols: list[str] = []
        self._rest_poll_interval = max(0, int(rest_poll_interval))
        self._rest_timeframe = rest_timeframe
        self._rest_interval_ms = max(1, intervalo_a_segundos(rest_timeframe)) * 1000
        self._rest_freeze_tolerance_ms = (
            int(rest_freeze_tolerance) * 1000
            if rest_freeze_tolerance is not None and rest_freeze_tolerance > 0
            else inactivity_timeout * 1000
        )
        self._rest_client: BinanceClient | None = rest_client
        self._own_rest_client = rest_client is None
        ttl = puntajes_ttl if puntajes_ttl is not None else max(int(inactivity_timeout), 60)
        self._puntajes_store = puntajes_store or PuntajeStore(ttl_seconds=ttl)
        self._puntajes_loaded = False
        self._puntaje_lock = asyncio.Lock()
        self._volume_window = max(3, int(volume_window))
        self._volume_baseline_min = max(1, int(volume_baseline_min))
        self._volume_extreme_multiplier = max(1.0, float(volume_extreme_multiplier))
        self._volume_history: DefaultDict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self._volume_window)
        )
        self._score_window = max(10, int(score_window))
        self._score_baseline_min = max(5, int(score_baseline_min))
        self._score_history: DefaultDict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self._score_window)
        )

    def actualizar_datos_externos(self, symbol: str, datos: dict) -> None:
        actual = _DATOS_EXTERNOS.setdefault(symbol, {})
        actual.update(datos)

    async def _hydrate_puntajes(self) -> None:
        if self._puntajes_loaded:
            return
        try:
            snapshots = await self._puntajes_store.load_all()
        except Exception as exc:
            log.warning(f'⚠️ No se pudieron cargar puntajes persistidos: {exc}')
            self._puntajes_loaded = True
            return
        async with self._puntaje_lock:
            for sym, snapshot in snapshots.items():
                _PUNTAJES[sym] = float(snapshot.value)
                metadata = dict(snapshot.metadata)
                metadata.setdefault('source', metadata.get('source', 'store'))
                metadata.setdefault('timeframe', metadata.get('timeframe', self._rest_timeframe))
                self.actualizar_datos_externos(sym, metadata)
                event_time = metadata.get('event_time') or metadata.get('close_time')
                if isinstance(event_time, (int, float)):
                    self._last_event_ts[sym] = int(event_time)
        self._puntajes_loaded = True

    async def _ensure_rest_client(self) -> BinanceClient | None:
        if self._rest_client is None and self._own_rest_client:
            try:
                self._rest_client = obtener_cliente()
            except Exception as exc:
                log.warning(f'⚠️ No fue posible inicializar cliente REST: {exc}')
                return None
        return self._rest_client

    async def _update_puntaje(
        self,
        symbol: str,
        puntaje: float,
        *,
        source: str,
        metadata: dict | None = None,
        notify: bool = False,
    ) -> None:
        sym = symbol.upper()
        meta = dict(metadata or {})
        meta.setdefault('source', source)
        meta.setdefault('timeframe', meta.get('timeframe', self._rest_timeframe))
        event_time = meta.get('event_time') or meta.get('close_time')
        if isinstance(event_time, (int, float)):
            self._last_event_ts[sym] = int(event_time)
        async with self._puntaje_lock:
            _PUNTAJES[sym] = float(puntaje)
            self.actualizar_datos_externos(sym, meta)
        self._record_context_metrics(sym, source, float(puntaje), meta)
        try:
            await self._puntajes_store.set(sym, float(puntaje), meta)
        except Exception as exc:
            log.warning(f'⚠️ No se pudo persistir puntaje {sym}: {exc}')
        if notify and self._handler_actual is not None:
            try:
                await self._handler_actual(sym, float(puntaje))
                tick('context_stream' if source == 'ws' else 'context_rest_probe')
            except Exception as exc:
                log.warning(f'⚠️ Handler contexto {sym} falló: {exc}')

    async def _apply_rest_snapshot(self, symbol: str, snapshot: dict) -> None:
        open_price = float(snapshot.get('open') or 0.0)
        volume = float(snapshot.get('volume') or 0.0)
        if open_price <= 0.0 or volume < 1e-08:
            return
        close_price = float(snapshot.get('close') or 0.0)
        resultado = self._calcular_puntaje_stream(symbol, open_price, close_price, volume)
        if resultado is None:
            return
        puntaje, calculo_meta = resultado
        await self._update_puntaje(
            symbol,
            puntaje,
            source='rest',
            metadata={**snapshot, **calculo_meta},
            notify=True,
        )
        event_reference = snapshot.get('event_time') or snapshot.get('close_time')
        if isinstance(event_reference, (int, float)):
            await self._handle_freeze_detection(symbol, int(event_reference))

    async def _handle_freeze_detection(self, symbol: str, reference_ts: int) -> None:
        tolerance = self._rest_freeze_tolerance_ms
        if tolerance is None or reference_ts <= 0:
            return
        last_event = self._last_event_ts.get(symbol.upper())
        if last_event is None or (reference_ts - last_event) <= tolerance:
            return
        log.warning(
            "🔄 Stream contexto %s detectado desfasado (%sms); reiniciando", symbol, reference_ts - last_event
        )
        await self._restart_stream(symbol, reason='rest_freeze')

    async def _restart_stream(self, symbol: str, *, reason: str) -> None:
        task = self._tasks.get(symbol)
        if task is None:
            return
        log.warning(f'🔁 Reiniciando stream contexto {symbol} (razón: {reason})')
        task.cancel()
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, timeout=self.cancel_timeout)
        if not self._running or self._handler_actual is None:
            return
        await asyncio.sleep(self.connection_delay)
        self._tasks[symbol] = supervised_task(
            lambda sym=symbol: self._stream(sym, self._handler_actual),
            name=f"context_stream_{symbol}",
        )
        ahora = datetime.now(UTC)
        self._last[symbol] = ahora
        self._last_monotonic[symbol] = time.monotonic()

    async def _rest_probe_loop(self) -> None:
        while self._running:
            if self._rest_poll_interval <= 0:
                await asyncio.sleep(self.monitor_interval)
                continue
            await asyncio.sleep(self._rest_poll_interval)
            if not self._running:
                break
            cliente = await self._ensure_rest_client()
            if cliente is None:
                continue
            symbols = list(self._symbols)
            for sym in symbols:
                try:
                    candles = await fetch_ohlcv_async(
                        cliente,
                        sym,
                        self._rest_timeframe,
                        limit=1,
                    )
                except Exception as exc:
                    log.warning(f'⚠️ Error consultando REST para {sym}: {exc}')
                    continue
                if not candles:
                    continue
                candle = candles[-1]
                open_time = int(float(candle[0]))
                open_price = float(candle[1])
                high = float(candle[2])
                low = float(candle[3])
                close_price = float(candle[4])
                volume = float(candle[5])
                snapshot = {
                    'source': 'rest',
                    'open_time': open_time,
                    'close_time': open_time + self._rest_interval_ms,
                    'event_time': open_time + self._rest_interval_ms,
                    'open': open_price,
                    'close': close_price,
                    'high': high,
                    'low': low,
                    'volume': volume,
                    'timeframe': self._rest_timeframe,
                }
                await self._apply_rest_snapshot(sym, snapshot)
            with contextlib.suppress(Exception):
                await self._puntajes_store.purge_expired()

    async def _stream(
        self, symbol: str, handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        symbol_norm = symbol.replace('/', '').lower()
        url = self.url_template.format(symbol=symbol_norm)
        intentos = 0
        while self._running:
            try:
                # SSL por defecto si no se proporcionó (endurece handshake)
                connect_ssl = self.ssl_context
                if connect_ssl is None:
                    connect_ssl = ssl.create_default_context()

                connect_params = {
                    "open_timeout": self.open_timeout,
                    # ✅ dejamos que 'websockets' gestione los pings (no keepalive manual)
                    "ping_interval": self.ping_interval,
                    "ping_timeout": self.ping_timeout,
                    "close_timeout": 5,
                    "max_queue": 0,       # backpressure interno inmediato (coherente con tus colas externas)
                    "compression": "deflate",
                    "ssl": connect_ssl,
                }

                async with websockets.connect(url, **connect_params) as ws:
                    log.info(f'🔌 Contexto conectado para {symbol}')
                    self._last[symbol] = datetime.now(UTC)
                    self._last_monotonic[symbol] = time.monotonic()
                    intentos = 0

                    # ❌ Eliminado el keepalive propio.
                    # keeper = asyncio.create_task(_keepalive(ws, symbol, self.ping_interval))

                    try:
                        async for msg in ws:
                            try:
                                self._last[symbol] = datetime.now(UTC)
                                self._last_monotonic[symbol] = time.monotonic()
                                data = json.loads(msg)
                                vela = data.get('k')
                                if vela is None:
                                    continue
                                close = float(vela.get('c', 0.0))
                                open_ = float(vela.get('o', 0.0))
                                vol = float(vela.get('v', 0.0))
                                if isclose(open_, 0.0, rel_tol=1e-12, abs_tol=1e-12) or vol < 1e-08:
                                    continue
                                resultado = self._calcular_puntaje_stream(symbol, open_, close, vol)
                                if resultado is None:
                                    continue
                                puntaje, calculo_meta = resultado
                                open_time = int(float(vela.get('t') or 0))
                                close_time = int(float(vela.get('T') or vela.get('close_time') or 0))
                                event_time = int(float(data.get('E') or vela.get('E') or close_time or 0))
                                metadata = {
                                    'source': 'ws',
                                    'open': open_,
                                    'close': close,
                                    'volume': vol,
                                    'open_time': open_time,
                                    'close_time': close_time,
                                    'event_time': event_time or close_time,
                                    'timeframe': str(vela.get('i') or self._rest_timeframe),
                                }
                                metadata.update(calculo_meta)
                                await self._update_puntaje(symbol, puntaje, source='ws', metadata=metadata, notify=False)
                                try:
                                    await handler(symbol, puntaje)
                                    tick('context_stream')
                                except Exception as e:
                                    log.warning(f'⚠️ Handler contexto {symbol} falló: {e}')
                            except asyncio.CancelledError:
                                log.info(f'🛑 Stream contexto {symbol} cancelado (mensaje).')
                                raise
                            except Exception as e:
                                log.warning(f'⚠️ Error procesando contexto de {symbol}: {e}')
                                self._record_parsing_error(symbol, stage="ws_message", exc=e)
                    finally:
                        # if keeper:
                        #     keeper.cancel()
                        #     try:
                        #         await keeper
                        #     except Exception as e:
                        #         log.debug(f'Error al esperar keepalive cancelado: {e}')
                        log.debug(f'Tareas activas tras cierre: {len(asyncio.all_tasks())}')
            except asyncio.CancelledError:
                log.info(f'🛑 Stream contexto {symbol} cancelado')
                raise
            except Exception as e:
                intentos += 1
                backoff = calcular_backoff(intentos, base=self.backoff_base, max_seg=self.backoff_cap)
                log.warning(
                    "⚠️ WS contexto %s intento %s falló: %s | backoff %.1fs url=%s",
                    symbol,
                    intentos,
                    e,
                    backoff,
                    url,
                )
                registro_metrico.registrar(
                    "context_ws_reconnect_total",
                    {"symbol": symbol, "attempt": intentos, "backoff_s": backoff},
                )
                if intentos >= self.max_retries:
                    log.error(
                        "🚫 Contexto %s superó %s fallos consecutivos; enfriando %.1fs",
                        symbol,
                        self.max_retries,
                        self.circuit_breaker,
                    )
                    await asyncio.sleep(self.circuit_breaker)
                    intentos = 0
                else:
                    await asyncio.sleep(backoff)
            else:
                break

    def _record_context_metrics(
        self,
        symbol: str,
        source: str,
        puntaje: float,
        metadata: dict,
    ) -> None:
        """Actualiza métricas de frescura, distribución y volumen."""

        sym = symbol.upper()
        event_reference = metadata.get('event_time') or metadata.get('close_time')
        now = time.time()
        if isinstance(event_reference, (int, float)):
            event_ts = float(event_reference) / 1000.0
        else:
            event_ts = now
        CONTEXT_LAST_UPDATE_SECONDS.labels(sym).set(event_ts)
        latency = max(now - event_ts, 0.0)
        CONTEXT_UPDATE_LATENCY_SECONDS.labels(sym, source).observe(latency)
        CONTEXT_SCORE_DISTRIBUTION.labels(sym).observe(float(puntaje))
        volume = metadata.get('volume')
        try:
            volume_value = float(volume)
        except (TypeError, ValueError):
            volume_value = 0.0
        self._update_volume_statistics(sym, source, volume_value)

    def _update_volume_statistics(self, symbol: str, source: str, volume: float) -> None:
        """Evalúa si ``volume`` es extremo y actualiza las métricas correspondientes."""

        if volume <= 0.0:
            return
        history = self._volume_history[symbol]
        if len(history) >= self._volume_baseline_min:
            mediana = statistics.median(history)
            if mediana > 0.0 and volume >= mediana * self._volume_extreme_multiplier:
                CONTEXT_VOLUME_EXTREME_TOTAL.labels(symbol, source).inc()
                log.warning(
                    "📈 Volumen extremo detectado en %s: %.6f (mediana reciente %.6f)",
                    symbol,
                    volume,
                    mediana,
                )
        history.append(volume)

    def _obtener_volumen_base(self, symbol: str, volume: float) -> float:
        """Determina un volumen de referencia robusto para el símbolo dado."""

        history = self._volume_history[symbol]
        if len(history) >= self._volume_baseline_min:
            base = statistics.median(history)
        elif history:
            base = statistics.fmean(history)
        else:
            base = volume
        if base <= 0.0:
            base = volume if volume > 0.0 else 1.0
        return float(base)

    def _calcular_puntaje_stream(
        self,
        symbol: str,
        open_price: float,
        close_price: float,
        volume: float,
    ) -> tuple[float, dict[str, float]] | None:
        """Calcula el puntaje normalizado (z-score) para el stream del símbolo."""

        if (
            open_price <= 0.0
            or close_price <= 0.0
            or math.isclose(open_price, 0.0, rel_tol=1e-12, abs_tol=1e-12)
            or volume < 1e-08
        ):
            return None

        try:
            retorno_log = math.log(close_price / open_price)
        except ValueError:
            return None

        base_volume = self._obtener_volumen_base(symbol, volume)
        volume_ratio = volume / base_volume if base_volume > 0.0 else 1.0
        volume_ratio = max(volume_ratio, 1e-09)
        weight = math.log1p(volume_ratio)
        weighted_return = retorno_log * weight

        history = self._score_history[symbol]
        puntaje = weighted_return
        if len(history) >= self._score_baseline_min:
            media = statistics.fmean(history)
            dispersion = statistics.pstdev(history)
            if dispersion > 1e-12:
                puntaje = (weighted_return - media) / dispersion
            else:
                puntaje = 0.0

        history.append(weighted_return)

        metadata = {
            'retorno_log': float(retorno_log),
            'volume_ratio': float(volume_ratio),
            'weight': float(weight),
            'weighted_return': float(weighted_return),
        }
        return float(puntaje), metadata

    def _record_parsing_error(self, symbol: str, stage: str, exc: Exception | str) -> None:
        """Incrementa las métricas asociadas a errores de parseo."""

        CONTEXT_PARSING_ERRORS_TOTAL.labels(symbol.upper(), stage).inc()
        log.debug("Context parsing error [%s - %s]: %s", symbol, stage, exc)

    async def _monitor_inactividad(self) -> None:
        """Vigila la actividad de los streams y los reinicia al quedar inactivos."""
        try:
            while True:
                tick('context_monitor')
                await asyncio.sleep(self.monitor_interval)
                if not self._running:
                    break
                ahora = time.monotonic()
                for sym, task in list(self._tasks.items()):
                    ultimo = self._last_monotonic.get(sym)
                    if task.done():
                        log.warning(f'🔄 Stream contexto {sym} finalizado; reiniciando')
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self._stream(sym, self._handler_actual),
                            name=f"context_stream_{sym}",
                        )
                        continue
                    if ultimo is not None and (ahora - ultimo) > self.inactivity_timeout:
                        # Si aún no hemos calculado ningún puntaje, probablemente no llegaron kline cerrados:
                        # evita bucles de reinicio ruidosos.
                        if sym not in _PUNTAJES:
                            log.debug(f'⏳ Contexto {sym} sin datos recientes; omitiendo reinicio')
                            self._last[sym] = datetime.now(UTC)
                            self._last_monotonic[sym] = ahora
                            continue
                        log.warning(f'🔄 Stream contexto {sym} inactivo; reiniciando')
                        task.cancel()
                        try:
                            await task
                        except Exception:
                            pass
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self._stream(sym, self._handler_actual),
                            name=f"context_stream_{sym}",
                        )
        except asyncio.CancelledError:
            pass

    async def escuchar(
        self, symbols: Iterable[str], handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        """Inicia un stream supervisado por cada símbolo."""
        await self.detener()
        self._handler_actual = handler
        self._symbols = list(symbols)
        self._running = True
        await self._hydrate_puntajes()
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_inactividad())
        if self._rest_poll_interval > 0:
            if self._rest_task is None or self._rest_task.done():
                self._rest_task = supervised_task(self._rest_probe_loop, name="context_rest_probe")
        for sym in self._symbols:
            if sym in self._tasks:
                log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                continue
            self._tasks[sym] = supervised_task(
                lambda sym=sym: self._stream(sym, handler),
                name=f"context_stream_{sym}",
            )
            # pequeño retraso entre conexiones para evitar estampida
            await asyncio.sleep(self.connection_delay)
        try:
            while self._running:
                await asyncio.sleep(1)
        finally:
            self._running = False

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        if self._rest_task and not self._rest_task.done():
            self._rest_task.cancel()
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
                await asyncio.wait_for(self._rest_task, timeout=self.cancel_timeout)
        self._rest_task = None
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(self._monitor_task, timeout=self.cancel_timeout)
            except asyncio.TimeoutError:
                log.warning('🧟 Timeout cancelando monitor global (tarea zombie)')
            except Exception:
                pass
        self._monitor_task = None

        for task in self._tasks.values():
            task.cancel()
        for nombre, task in list(self._tasks.items()):
            if task.done():
                continue
            try:
                await asyncio.wait_for(task, timeout=self.cancel_timeout)
            except asyncio.TimeoutError:
                log.warning(f'🧟 Timeout cancelando stream {nombre} (tarea zombie)')
            except Exception:
                pass
        self._tasks.clear()
        self._symbols = []
        if self._own_rest_client and self._rest_client is not None:
            with contextlib.suppress(Exception):
                await self._rest_client.close()
            self._rest_client = None


__all__ = [
    'StreamContexto',
    'obtener_puntaje_contexto',
    'obtener_todos_puntajes',
    'obtener_datos_externos',
]

