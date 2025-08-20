import asyncio
import json
import random
from datetime import datetime, timezone
from collections import deque
import socket
import time
from typing import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

UTC = timezone.utc

from core.utils.utils import configurar_logger, intervalo_a_segundos
# Si en el futuro se escuchan streams de profundidad (@depthUpdate),
# será necesario validar la secuencia de mensajes con el esquema
# ``U <= lastUpdateId + 1 <= u`` y obtener un snapshot inicial vía
# ``/api/v3/depth`` antes de procesar las actualizaciones.
from core.supervisor import tick, tick_data
from binance_api.cliente import fetch_ohlcv_async


class InactividadTimeoutError(Exception):
    """Señala que el WebSocket se cerró por falta de datos."""

    pass

log = configurar_logger('websocket')

RECONNECT_WINDOW = 300  # 5 minutos
RECONNECT_THRESHOLD = 250
_reconnect_history: deque[float] = deque()
MAX_BACKOFF = 60
MAX_BACKFILL_CANDLES = 100  # límite para backfill para evitar saturación
BACKFILL_CONCURRENCY = 3
_backfill_semaphore = asyncio.Semaphore(BACKFILL_CONCURRENCY)
MESSAGE_QUEUE_SIZE = 100
CALLBACK_TIMEOUT = 5

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
    log.debug('➡️ Entrando en normalizar_symbolo()')
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

        h['ultimo_timestamp'] = await _rellenar_gaps(
            h['callback'],
            symbol,
            h.get('ultimo_timestamp'),
            h.get('ultimo_cierre'),
            vela['timestamp'],
            h['intervalo_ms'],
        )
        try:
            await asyncio.wait_for(h['callback'](vela), timeout=callback_timeout)
        except Exception as e:
            log.warning(f'❌ Callback falló: {e}')
            queue.task_done()
            continue
        h['ultimo_timestamp'] = vela['timestamp']
        h['ultimo_cierre'] = vela['close']
        last_message[symbol] = time.monotonic()
        tick('data_feed')
        queue.task_done()



async def _gestionar_ws(
    url: str,
    handlers: dict,
    last_message: dict[str, float],
    tiempo_maximo: int,
    ping_interval: int,
    mensaje_timeout: int | None,
    cliente=None,
):
    """Gestiona un WebSocket genérico con reconexión y backfill.

    Cada elemento de ``handlers`` debe contener:
    ``callback`` -- función async que recibe la vela normalizada.

    ``parser`` -- función que recibe el mensaje crudo y devuelve la vela
    normalizada o ``None``.

    ``ultimo_timestamp`` y ``ultimo_cierre`` -- utilizados para generar
    velas sintéticas y backfillear en reconexiones.

    ``intervalo`` e ``intervalo_ms`` -- necesarios para el backfill vía
    :func:`fetch_ohlcv_async`.
    """
    fallos_consecutivos = 0
    total_reintentos = 0
    backoff = 5
    while True:
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=10,
                    close_timeout=10,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=2 ** 20,
                ),
                timeout=15,
            )
            log.info(
                f"🔌 WebSocket conectado a {url} a las {datetime.now(UTC).isoformat()}"
            )
            _habilitar_tcp_keepalive(ws)
            fallos_consecutivos = 0
            backoff = 5
            for s in handlers:
                last_message[s] = time.monotonic()
            watchdogs = [
                asyncio.create_task(_watchdog(ws, s, last_message, tiempo_maximo))
                for s in handlers
            ]
            keeper = asyncio.create_task(
                _keepalive(ws, 'combined' if len(handlers) > 1 else next(iter(handlers)), ping_interval)
            )

            handlers_by_norm = {normalizar_symbolo(s): s for s in handlers}
            message_queue: asyncio.Queue = asyncio.Queue(maxsize=MESSAGE_QUEUE_SIZE)
            consumer = asyncio.create_task(
                _procesar_cola(message_queue, handlers, last_message, handlers_by_norm)
            )

            backfill_tasks = []
            if cliente:
                for s, h in handlers.items():
                    ts = h.get('ultimo_timestamp')
                    if ts is None:
                        continue

                    async def _backfill_symbol(symbol=s, h=h):
                        intentos = 0
                        espera = 1
                        while True:
                            try:
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
                                        await h['callback'](
                                            {
                                                'symbol': symbol,
                                                'timestamp': tss,
                                                'open': float(o[1]),
                                                'high': float(o[2]),
                                                'low': float(o[3]),
                                                'close': float(o[4]),
                                                'volume': float(o[5]),
                                            }
                                        )
                                        h['ultimo_timestamp'] = tss
                                        h['ultimo_cierre'] = float(o[4])
                                break
                            except Exception as e:
                                intentos += 1
                                if intentos >= 3:
                                    log.warning(
                                        f'❌ Error al backfillear {symbol} tras {intentos} intentos: {e}'
                                    )
                                    tick('data_feed')
                                    break
                                await asyncio.sleep(espera + random.random())
                                espera *= 2

                    backfill_tasks.append(asyncio.create_task(_backfill_symbol()))
            try:
                while True:
                    try:
                        if mensaje_timeout:
                            msg = await asyncio.wait_for(ws.recv(), timeout=mensaje_timeout)
                        else:
                            msg = await ws.recv()
                        try:
                            message_queue.put_nowait(msg)
                        except asyncio.QueueFull:
                            log.warning('📦 Cola de mensajes llena, descartando mensaje')
                            tick('data_feed')
                        continue
                    except asyncio.TimeoutError:
                        log.warning(
                            f'⏰ Sin datos en {mensaje_timeout}s, forzando reconexión'
                        )
                        await ws.close()
                        break
                    except ConnectionClosed as e:
                        log.warning(
                            f"🚪 WebSocket cerrado — Código: {e.code}, Motivo: {e.reason}"
                        )
                        await ws.close()
                        break
                    except Exception as e:
                        log.warning(f'❌ Error recibiendo datos: {e}')
                        await ws.close()
                        break

                    pass
            finally:
                log.info(
                    f"🔻 WebSocket desconectado de {url} a las {datetime.now(UTC).isoformat()}"
                )
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
                        log.debug(
                            f'Error al esperar tarea cancelada: {e}'
                        )
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
        except Exception:
            fallos_consecutivos += 1
            total_reintentos += 1
            log.exception('❌ Error en WebSocket')
            log.info(
                f'🔁 Reintentando conexión en {backoff} segundos... (total reintentos: {total_reintentos})'
            )
            _registrar_reconexion()
            tick('data_feed')
            await asyncio.sleep(backoff + random.random())
            previo = backoff
            backoff = min(MAX_BACKOFF, backoff * 2)
            if backoff == MAX_BACKOFF and previo < MAX_BACKOFF:
                log.warning(f'⚠️ Backoff máximo alcanzado: {MAX_BACKOFF}s')
            elif fallos_consecutivos >= 5:
                log.warning(
                    f'⏳ {fallos_consecutivos} fallos consecutivos. Nuevo backoff: {backoff}s'
                )


async def escuchar_velas(
    symbol: str,
    intervalo: str,
    callback,
    last_message: dict[str, float] | None = None,
    tiempo_maximo: int | None = None,
    ping_interval: int | None = None,
    cliente=None,
    mensaje_timeout: int | None = None,
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
        tiempo_maximo = max(intervalo_a_segundos(intervalo) * 4, 60)
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
            'ultimo_timestamp': None,
            'ultimo_cierre': None,
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
        tiempo_maximo = max(intervalo_a_segundos(intervalo) * 4, 60)
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
            'ultimo_timestamp': None,
            'ultimo_cierre': None,
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
    )


async def _watchdog(
    ws,
    symbol: str,
    last_message: dict[str, float],
    tiempo_maximo: int,
):
    """Cierra ``ws`` si no se reciben datos por ``tiempo_maximo`` segundos."""
    log.debug('➡️ Entrando en _watchdog()')
    """
    Si no llega ninguna vela en tiempo_maximo (segundos), cierra el websocket para reiniciar.
    """
    try:
        intervalo = min(5, max(1, tiempo_maximo / 5))
        while True:
            await asyncio.sleep(intervalo)
            ultimo = last_message.get(symbol)
            if ultimo is None:
                last_message[symbol] = time.monotonic()
                continue
            if time.monotonic() - ultimo > tiempo_maximo:
                log.warning(
                    f'⚠️ No se recibieron velas en {tiempo_maximo}s para {symbol}, forzando reconexión.'
                )
                await ws.close()
                tick('data_feed')
                tick_data(symbol)
                raise InactividadTimeoutError(
                    f'Sin velas en {tiempo_maximo}s para {symbol}'
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
