import asyncio
import json
import random
from datetime import datetime, timezone
from collections import deque
import socket
import time
from typing import Awaitable, Callable

import ssl
import websockets
from websockets.exceptions import ConnectionClosed
import certifi
import resource

UTC = timezone.utc

from core.utils.utils import configurar_logger, intervalo_a_segundos
# Si en el futuro se escuchan streams de profundidad (@depthUpdate),
# será necesario validar la secuencia de mensajes con el esquema
# ``U <= lastUpdateId + 1 <= u`` y obtener un snapshot inicial vía
# ``/api/v3/depth`` antes de procesar las actualizaciones.
from core.supervisor import tick, tick_data, registrar_ping
from binance_api.cliente import fetch_ohlcv_async
from core.registro_metrico import registro_metrico
from core.utils.backoff import calcular_backoff


class InactividadTimeoutError(Exception):
    """Señala que el WebSocket se cerró por falta de datos."""

    pass

log = configurar_logger('websocket')

def _ajustar_limite_archivos(min_limit: int = 4096) -> None:
    """Intenta aumentar el límite de descriptores si es demasiado bajo."""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < min_limit:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min_limit, hard))
            log.debug(f'Límite de archivos incrementado a {min_limit}')
    except Exception as e:  # pragma: no cover - fallbacks en entornos no POSIX
        log.debug(f'No se pudo ajustar el límite de archivos: {e}')

_ajustar_limite_archivos()

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

RECONNECT_WINDOW = 300  # 5 minutos
RECONNECT_THRESHOLD = 250
_reconnect_history: deque[float] = deque()
MAX_BACKOFF = 60
MAX_BACKFILL_CANDLES = 100  # límite para backfill para evitar saturación
BACKFILL_CONCURRENCY = 3
_backfill_semaphore = asyncio.Semaphore(BACKFILL_CONCURRENCY)
MESSAGE_QUEUE_SIZE = 1000
CALLBACK_TIMEOUT = 8

def _registrar_reconexion() -> None:
    """Registra un intento de reconexión y alerta si la tasa es elevada."""
    ahora = time.monotonic()
    _reconnect_history.append(ahora)
    while _reconnect_history and (ahora - _reconnect_history[0]) > RECONNECT_WINDOW:
        _reconnect_history.popleft()
    if len(_reconnect_history) > RECONNECT_THRESHOLD:
        log.warning(
            f'⚠️ Tasa de reconexión alta: {len(_reconnect_history)} en {RECONNECT_WINDOW // 60} min'
        )


def obtener_tasa_reconexion() -> int:
    """Devuelve la cantidad de reconexiones registradas en la ventana actual."""
    ahora = time.monotonic()
    while _reconnect_history and (ahora - _reconnect_history[0]) > RECONNECT_WINDOW:
        _reconnect_history.popleft()
    return len(_reconnect_history)


def normalizar_symbolo(symbol: str) -> str:
    return symbol.replace('/', '').lower()


INTERVALOS_VALIDOS = {'1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'}


async def _rellenar_gaps(
    emisor,
    symbol: str,
    ultimo_ts: int | None,
    ultimo_cierre: float | None,
    nuevo_ts: int,
    intervalo_ms: int,
):
    """Genera velas sintéticas para cubrir huecos entre ``ultimo_ts`` y ``nuevo_ts``.

    Las velas generadas tendrán volumen 0 y precios planos igualados a ``ultimo_cierre``.
    Devuelve el último timestamp emitido (o el original si no había hueco).
    """
    if ultimo_ts is None or ultimo_cierre is None:
        return ultimo_ts

    gap = ultimo_ts + intervalo_ms
    while gap < nuevo_ts:
        await emisor(
            {
                'symbol': symbol,
                'timestamp': gap,
                'open': ultimo_cierre,
                'high': ultimo_cierre,
                'low': ultimo_cierre,
                'close': ultimo_cierre,
                'volume': 0.0,
            }
        )
        ultimo_ts = gap
        gap += intervalo_ms

    return ultimo_ts



async def _procesar_cola(
    queue: asyncio.Queue,
    handlers: dict,
    last_message: dict[str, float],
    handlers_by_norm: dict[str, str],
    callback_timeout: int = CALLBACK_TIMEOUT,
):
    while True:
        raw = await queue.get()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning(f'Mensaje JSON inválido: {raw}')
            queue.task_done()
            continue

        if 'stream' in data:
            stream = data['stream']
            norm = stream.split('@')[0]
            symbol = handlers_by_norm.get(norm)
            if not symbol:
                queue.task_done()
                continue
            payload = data.get('data', {})
        else:
            symbol = next(iter(handlers))
            payload = data

        h = handlers[symbol]
        try:
            vela = h['parser'](payload)
        except Exception as e:
            log.warning(f'❌ Error parseando mensaje: {e}')
            queue.task_done()
            continue

        if not vela:
            queue.task_done()
            continue

        # Relleno de gaps previo a emitir la vela actual
        h['ultimo_timestamp'] = await _rellenar_gaps(
            h['callback'],
            symbol,
            h.get('ultimo_timestamp'),
            h.get('ultimo_cierre'),
            vela['timestamp'],
            h['intervalo_ms'],
        )

        # Emitimos la vela actual con timeout defensivo
        try:
            await asyncio.wait_for(h['callback'](vela), timeout=callback_timeout)
        except Exception as e:
            log.warning(f'❌ Callback falló: {e}')
            queue.task_done()
            continue

        # Actualizamos estado y latidos (incluye tick_data por símbolo)
        h['ultimo_timestamp'] = vela['timestamp']
        h['ultimo_cierre'] = vela['close']
        last_message[symbol] = time.monotonic()
        tick('data_feed')
        tick_data(symbol)  # <--- añadido: latido por símbolo en cada vela
        queue.task_done()




async def _gestionar_ws(
    url: str,
    handlers: dict,
    last_message: dict[str, float],
    tiempo_maximo: int,
    ping_interval: int,
    mensaje_timeout: int | None,
    cliente=None,
    backpressure: bool = False,
):
    """Gestiona un WebSocket genérico con reconexión y backfill (incluye bootstrap si ts=None)."""
    fallos_consecutivos = 0
    total_reintentos = 0
    backoff = 5
    primera_vez = True
    while True:
        try:
            if primera_vez:
                await asyncio.sleep(random.random())
                primera_vez = False
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=10,
                    close_timeout=10,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=2 ** 20,
                    ssl=_SSL_CONTEXT,
                ),
                timeout=15,
            )
            log.info(
                f"🔌 WebSocket conectado a {url} a las {datetime.now(UTC).isoformat()}"
            )
            _habilitar_tcp_keepalive(ws)
            fallos_consecutivos = 0
            backoff = 5

            # Inicializa marcadores de último mensaje para los watchdogs
            for s in handlers:
                last_message[s] = time.monotonic()

            # Watchdogs por símbolo + keepalive ping
            watchdogs = [
                asyncio.create_task(_watchdog(ws, s, last_message, tiempo_maximo))
                for s in handlers
            ]
            keeper = asyncio.create_task(
                _keepalive(ws, 'combined' if len(handlers) > 1 else next(iter(handlers)), ping_interval)
            )

            handlers_by_norm = {normalizar_symbolo(s): s for s in handlers}
            message_queue: asyncio.Queue = asyncio.Queue(maxsize=MESSAGE_QUEUE_SIZE)
            queue_discards: dict[str, int] = {}

            def _symbol_from_msg(msg: str) -> str | None:
                try:
                    data = json.loads(msg)
                except Exception:
                    return None
                if 'stream' in data:
                    norm = data['stream'].split('@')[0]
                    return handlers_by_norm.get(norm)
                return next(iter(handlers))
            consumer = asyncio.create_task(
                _procesar_cola(message_queue, handlers, last_message, handlers_by_norm)
            )

            # === Bootstrap & Backfill =========================================
            backfill_tasks = []
            if cliente:
                for s, h in handlers.items():
                    ts = h.get('ultimo_timestamp')

                    async def _backfill_symbol(symbol=s, h=h, ts=ts):
                        """
                        - Si ts es None: bootstrap (trae últimas K velas recientes).
                        - Si ts no es None: backfill desde ts+1 hasta ahora (máx. MAX_BACKFILL_CANDLES).
                        """
                        intentos = 0
                        espera = 1
                        while True:
                            try:
                                if ts is None:
                                    # Bootstrap: velas recientes para arrancar con contexto
                                    K = min(60, MAX_BACKFILL_CANDLES)
                                    async with _backfill_semaphore:
                                        ohlcv = await asyncio.wait_for(
                                            fetch_ohlcv_async(
                                                cliente,
                                                symbol=symbol,
                                                timeframe=h['intervalo'],
                                                since=None,
                                                limit=K,
                                            ),
                                            timeout=10,
                                        )
                                    # Empuja en orden (antiguo -> reciente)
                                    for o in ohlcv:
                                        tss = o[0]
                                        vela = {
                                            'symbol': symbol,
                                            'timestamp': tss,
                                            'open': float(o[1]),
                                            'high': float(o[2]),
                                            'low': float(o[3]),
                                            'close': float(o[4]),
                                            'volume': float(o[5]),
                                        }
                                        await h['callback'](vela)
                                        h['ultimo_timestamp'] = tss
                                        h['ultimo_cierre'] = vela['close']
                                        tick('data_feed')
                                        tick_data(symbol)
                                else:
                                    # Backfill normal desde ts conocido
                                    ahora = int(datetime.now(UTC).timestamp() * 1000)
                                    faltan = max(1, (ahora - h['ultimo_timestamp']) // h['intervalo_ms'])
                                    limite = min(faltan, MAX_BACKFILL_CANDLES)
                                    async with _backfill_semaphore:
                                        ohlcv = await asyncio.wait_for(
                                            fetch_ohlcv_async(
                                                cliente,
                                                symbol=symbol,
                                                timeframe=h['intervalo'],
                                                since=h['ultimo_timestamp'] + 1,
                                                limit=limite,
                                            ),
                                            timeout=10,
                                        )
                                    for o in ohlcv:
                                        tss = o[0]
                                        if tss > h['ultimo_timestamp']:
                                            h['ultimo_timestamp'] = await _rellenar_gaps(
                                                h['callback'],
                                                symbol,
                                                h['ultimo_timestamp'],
                                                h['ultimo_cierre'],
                                                tss,
                                                h['intervalo_ms'],
                                            )
                                            vela = {
                                                'symbol': symbol,
                                                'timestamp': tss,
                                                'open': float(o[1]),
                                                'high': float(o[2]),
                                                'low': float(o[3]),
                                                'close': float(o[4]),
                                                'volume': float(o[5]),
                                            }
                                            await h['callback'](vela)
                                            h['ultimo_timestamp'] = tss
                                            h['ultimo_cierre'] = vela['close']
                                            tick('data_feed')
                                            tick_data(symbol)
                                break
                            except Exception as e:
                                intentos += 1
                                if intentos >= 3:
                                    log.warning(f'❌ Error al backfillear {symbol} tras {intentos} intentos: {e}')
                                    tick('data_feed')
                                    break
                                await asyncio.sleep(espera + random.random())
                                espera *= 2

                    backfill_tasks.append(asyncio.create_task(_backfill_symbol()))
            # ===================================================================

            try:
                while True:
                    try:
                        if mensaje_timeout:
                            msg = await asyncio.wait_for(ws.recv(), timeout=mensaje_timeout)
                        else:
                            msg = await ws.recv()
                        qsize = message_queue.qsize()
                        symbol = None
                        if message_queue.maxsize and qsize > message_queue.maxsize * 0.8:
                            symbol = _symbol_from_msg(msg)
                            if symbol:
                                log.warning(
                                    f"[{symbol}] queue_size={qsize}/{message_queue.maxsize}"
                                )
                        try:
                            if backpressure:
                                await asyncio.wait_for(message_queue.put(msg), timeout=0.5)
                            else:
                                message_queue.put_nowait(msg)
                        except (asyncio.TimeoutError, asyncio.QueueFull):
                            symbol = symbol or _symbol_from_msg(msg)
                            if symbol:
                                queue_discards[symbol] = queue_discards.get(symbol, 0) + 1
                                log.warning(
                                    f"[{symbol}] Cola de mensajes llena, descartando mensaje ({queue_discards[symbol]})"
                                )
                                registro_metrico.registrar(
                                    'queue_discards',
                                    {'symbol': symbol, 'count': queue_discards[symbol]},
                                )
                            tick('data_feed')
                        continue
                    except asyncio.TimeoutError:
                        log.warning(f'⏰ Sin datos en {mensaje_timeout}s, forzando reconexión')
                        await ws.close()
                        break
                    except ConnectionClosed as e:
                        log.warning(f"🚪 WebSocket cerrado — Código: {e.code}, Motivo: {e.reason}")
                        await ws.close()
                        break
                    except Exception as e:
                        log.warning(f'❌ Error recibiendo datos: {e}')
                        await ws.close()
                        break
            finally:
                log.info(f"🔻 WebSocket desconectado de {url} a las {datetime.now(UTC).isoformat()}")
                for t in backfill_tasks:
                    try:
                        await t
                    except Exception as e:
                        log.debug(f'Error en backfill: {e}')
                        tick('data_feed')
                for t in watchdogs + [keeper, consumer]:
                    t.cancel()
                for t in watchdogs + [keeper, consumer]:
                    try:
                        await t
                    except InactividadTimeoutError:
                        raise
                    except Exception as e:
                        log.debug(f'Error al esperar tarea cancelada: {e}')
                        tick('data_feed')
                log.debug(f'Tareas activas tras cierre: {len(asyncio.all_tasks())}')
                try:
                    await ws.close()
                    await ws.wait_closed()
                except Exception as e:
                    log.debug(f'Error al cerrar WebSocket: {e}')
                    tick('data_feed')
        except asyncio.CancelledError:
            log.info('🛑 Conexión WebSocket cancelada.')
            break
        except Exception as e:
            fallos_consecutivos += 1
            total_reintentos += 1
            log.error(f'❌ Error en WebSocket: {e}')
            log.info(f'🔁 Reintentando conexión en {backoff} segundos... (total reintentos: {total_reintentos})')
            _registrar_reconexion()
            tick('data_feed')
            await asyncio.sleep(backoff + random.random())
            previo = backoff
            backoff = calcular_backoff(backoff, fallos_consecutivos, MAX_BACKOFF)
            if backoff == MAX_BACKOFF and previo < MAX_BACKOFF:
                log.warning(f'⚠️ Backoff máximo alcanzado: {MAX_BACKOFF}s')
            elif fallos_consecutivos >= 5:
                log.warning(f'⏳ {fallos_consecutivos} fallos consecutivos. Nuevo backoff: {backoff}s')



async def escuchar_velas(
    symbol: str,
    intervalo: str,
    callback,
    last_message: dict[str, float] | None = None,
    tiempo_maximo: int | None = None,
    ping_interval: int | None = None,
    cliente=None,
    mensaje_timeout: int | None = None,
    backpressure: bool = False,
    ultimo_timestamp: int | None = None,
    ultimo_cierre: float | None = None,
):
    """Escucha velas cerradas de ``symbol`` delegando la gestión al helper."""
    if not isinstance(symbol, str) or '/' not in symbol:
        raise ValueError(f'Símbolo inválido: {symbol}')
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError(f'Intervalo inválido: {intervalo}')
    url = f'wss://stream.binance.com:9443/ws/{normalizar_symbolo(symbol)}@kline_{intervalo}'
    if last_message is None:
        last_message = {}
    if tiempo_maximo is None:
        base_timeout = intervalo_a_segundos(intervalo) * 5
        if base_timeout < 300:
            log.info(
                f'⌛ Timeout de inactividad extendido a 300s para {symbol}'
            )
        tiempo_maximo = max(base_timeout, 300)
    if ping_interval is None:
        ping_interval = 30
    if mensaje_timeout is None:
        mensaje_timeout = tiempo_maximo
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000

    def parser(data):
        if data.get('e') != 'kline':
            return None
        k = data['k']
        if not k['x']:
            return None
        return {
            'symbol': symbol,
            'timestamp': k['t'],
            'open': float(k['o']),
            'high': float(k['h']),
            'low': float(k['l']),
            'close': float(k['c']),
            'volume': float(k['v']),
        }

    handlers = {
        symbol: {
            'callback': callback,
            'parser': parser,
            'ultimo_timestamp': ultimo_timestamp,
            'ultimo_cierre': ultimo_cierre,
            'intervalo': intervalo,
            'intervalo_ms': intervalo_ms,
        }
    }

    await _gestionar_ws(
        url,
        handlers,
        last_message,
        tiempo_maximo,
        ping_interval,
        mensaje_timeout,
        cliente,
        backpressure=backpressure,
    )


async def escuchar_velas_combinado(
    symbols: list[str],
    intervalo: str,
    handlers: dict[str, Callable[[dict], Awaitable[None]]],
    last_message: dict[str, float] | None = None,
    tiempo_maximo: int | None = None,
    ping_interval: int | None = None,
    cliente=None,
    mensaje_timeout: int | None = None,
    backpressure: bool = False,
    ultimos: dict[str, dict] | None = None,
):
    """Escucha velas de múltiples símbolos usando un stream combinado."""
    if not symbols:
        raise ValueError('Debe proporcionarse al menos un símbolo')
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError(f'Intervalo inválido: {intervalo}')
    normalizados = {normalizar_symbolo(s): s for s in symbols}
    for s in symbols:
        if '/' not in s:
            raise ValueError(f'Símbolo inválido: {s}')
        if s not in handlers:
            raise ValueError(f'Falta handler para {s}')
    streams = '/'.join(f"{n}@kline_{intervalo}" for n in normalizados)
    url = f'wss://stream.binance.com:9443/stream?streams={streams}'
    if last_message is None:
        last_message = {}
    if tiempo_maximo is None:
        base_timeout = intervalo_a_segundos(intervalo) * 5
        if base_timeout < 300:
            log.info(
                '⌛ Timeout de inactividad extendido a 300s para stream combinado'
            )
        tiempo_maximo = max(base_timeout, 300)
    if ping_interval is None:
        ping_interval = 30
    if mensaje_timeout is None:
        mensaje_timeout = tiempo_maximo
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
    ws_handlers = {}
    for s in symbols:
        def make_parser(sym):
            def parser(data):
                if data.get('e') != 'kline':
                    return None
                k = data['k']
                if not k['x']:
                    return None
                return {
                    'symbol': sym,
                    'timestamp': k['t'],
                    'open': float(k['o']),
                    'high': float(k['h']),
                    'low': float(k['l']),
                    'close': float(k['c']),
                    'volume': float(k['v']),
                }
            return parser

        ws_handlers[s] = {
            'callback': handlers[s],
            'parser': make_parser(s),
            'ultimo_timestamp': (ultimos.get(s, {}).get('ultimo_timestamp') if ultimos else None),
            'ultimo_cierre': (ultimos.get(s, {}).get('ultimo_cierre') if ultimos else None),
            'intervalo': intervalo,
            'intervalo_ms': intervalo_ms,
        }

    await _gestionar_ws(
        url,
        ws_handlers,
        last_message,
        tiempo_maximo,
        ping_interval,
        mensaje_timeout,
        cliente,
        backpressure=backpressure,
    )


async def _watchdog(
    ws,
    symbol: str,
    last_message: dict[str, float],
    tiempo_maximo: float,
):
    """Cierra el WS si no se reciben datos en ``tiempo_maximo`` segundos (usa reloj monotónico)."""
    try:
        # Revisión: intervalo de sondeo razonable y siempre >0
        intervalo = max(1.0, min(5.0, float(tiempo_maximo) / 5.0))
        while True:
            await asyncio.sleep(intervalo)

            ahora_mono = time.monotonic()
            ultimo = last_message.get(symbol)

            # Inicializa si aún no hay marca
            if ultimo is None:
                last_message[symbol] = ahora_mono
                continue

            # Autodefensa: si alguien metió un datetime u otro tipo, re-inicializa
            if not isinstance(ultimo, (int, float)):
                log.debug(f'⛑️ Corrigiendo tipo de last_message[{symbol}]={type(ultimo).__name__}')
                last_message[symbol] = ahora_mono
                continue

            elapsed = ahora_mono - float(ultimo)
            if elapsed > float(tiempo_maximo):
                log.warning(
                    f'⚠️ No se recibieron velas en {tiempo_maximo:.0f}s para {symbol}, forzando reconexión.'
                )
                try:
                    await ws.close()
                finally:
                    tick('data_feed')
                    tick_data(symbol)
                raise InactividadTimeoutError(
                    f'Sin velas en {tiempo_maximo:.0f}s para {symbol}'
                )

    except asyncio.CancelledError:
        raise
    except InactividadTimeoutError:
        raise
    except Exception as e:
        log.warning(f'Excepción inesperada en watchdog de {symbol}: {e}')
        tick('data_feed')
        tick_data(symbol)


async def _keepalive(ws, symbol, intervalo=30, log_interval=10):
    """Envía ping periódicamente para mantener viva la conexión."""
    contador = 0
    try:
        while True:
            await asyncio.sleep(intervalo)
            try:
                contador += 1
                log.debug(f'🏓 Enviando ping a {symbol}')
                inicio = time.perf_counter()
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=10)
                rtt = (time.perf_counter() - inicio) * 1000
                log.debug(f'🏓 Pong recibido de {symbol} ({rtt:.1f} ms)')
                registrar_ping(symbol, rtt)
                if contador % log_interval == 0:
                    log.info(f'📡 RTT ping {symbol}: {rtt:.1f} ms')
            except Exception as e:
                log.warning(f'❌ Ping falló para {symbol}: {e}')
                await ws.close()
                tick('data_feed')
                break
    except asyncio.CancelledError:
        raise

def _habilitar_tcp_keepalive(ws):
    """Activa el keep-alive del sistema operativo para ``ws`` si es posible."""
    try:
        sock = ws.transport.get_extra_info('socket')
        if not sock:
            return
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPIDLE'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        if hasattr(socket, 'TCP_KEEPINTVL'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
        if hasattr(socket, 'TCP_KEEPCNT'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4)
    except Exception as e:
        log.warning(f'No se pudo configurar TCP keep-alive: {e}')
        tick('data_feed')
