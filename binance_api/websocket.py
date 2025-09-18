import asyncio
import json
import os
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
try:
    import resource  # type: ignore
except Exception:  # pragma: no cover - fallback for non-POSIX systems
    resource = None

UTC = timezone.utc

from core.utils.utils import configurar_logger, intervalo_a_segundos
from core.supervisor import tick, tick_data, registrar_ping
from binance_api.cliente import fetch_ohlcv_async
from core.registro_metrico import registro_metrico
from core.utils.backoff import backoff_sleep


class InactividadTimeoutError(Exception):
    """Señala que el WebSocket se cerró por falta de datos."""
    pass


log = configurar_logger('websocket')


def _ajustar_limite_archivos(min_limit: int = 4096) -> None:
    """Intenta aumentar el límite de descriptores si es demasiado bajo."""
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < min_limit:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min_limit, hard))
            log.debug(f'Límite de archivos incrementado a {min_limit}')
    except Exception as e:  # pragma: no cover - fallbacks en entornos no POSIX
        # Aviso visible en caso de no poder elevar
        log.warning(f'No se pudo ajustar RLIMIT_NOFILE a {min_limit}: {e}')


_ajustar_limite_archivos()

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

RECONNECT_WINDOW = 300  # 5 minutos
RECONNECT_THRESHOLD = 100
_reconnect_history: deque[float] = deque()
MAX_BACKFILL_CANDLES = 100  # límite para backfill para evitar saturación
BACKFILL_CONCURRENCY = 3
_backfill_semaphore = asyncio.Semaphore(BACKFILL_CONCURRENCY)
MESSAGE_QUEUE_SIZE = 2000
CALLBACK_TIMEOUT = 8

PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "10"))
OPEN_TIMEOUT = int(os.getenv("WS_OPEN_TIMEOUT", "10"))
CLOSE_TIMEOUT = int(os.getenv("WS_CLOSE_TIMEOUT", "5"))
MAX_BACKOFF = float(os.getenv("WS_BACKOFF_MAX", "60"))
BACKOFF_BASE = float(os.getenv("WS_BACKOFF_BASE", "1.0"))
QUEUE_WARN_INTERVAL = float(os.getenv("WS_QUEUE_WARN_INTERVAL", "5.0"))

TIMEOUT_SIN_DATOS_FACTOR_MIN = 90  # antes era 300


def _calc_inactividad_timeout(intervalo_vela_s: int) -> int:
    return max(2 * intervalo_vela_s, TIMEOUT_SIN_DATOS_FACTOR_MIN)


USE_INTERNAL_KEEPALIVE = False


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


async def _resuscribir(
    ws,
    handlers: dict,
    timeout: float = 5,
    max_intentos: int = 3,
) -> None:
    """Envía suscripciones y espera ACK por símbolo tras reconectar.

    Si un símbolo no recibe ACK tras ``max_intentos`` se levanta una excepción
    para que el WebSocket se reinicie.
    """
    loop = asyncio.get_running_loop()
    for sym, h in handlers.items():
        stream = f"{normalizar_symbolo(sym)}@kline_{h['intervalo']}"
        intentos = 0
        while True:
            intentos += 1
            req_id = random.randint(1, 1_000_000)
            mensaje = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": req_id,
            }
            await ws.send(json.dumps(mensaje))
            inicio = time.perf_counter()
            try:
                # Ventana de espera: lee varios mensajes hasta encontrar el ACK correcto
                deadline = loop.time() + timeout
                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError("ACK no recibido dentro del timeout")
                    ack_raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    if _es_ack(ack_raw, req_id):
                        elapsed_ms = (time.perf_counter() - inicio) * 1000
                        log.info(
                            "ACK resuscripción",
                            extra={
                                "sym": sym,
                                "attempt": intentos,
                                "elapsed_ms": round(elapsed_ms, 1),
                            },
                        )
                        break
                    # Mensaje no-ACK durante resuscripción (puede ser kline): lo descartamos aquí
                    # para mantener la simplicidad. El consumidor retomará al reconectar.
                    log.debug("Mensaje no-ACK recibido durante resuscripción (descartado temporalmente)")
                break
            except Exception as e:  # noqa: PERF203 - logging needed
                elapsed_ms = (time.perf_counter() - inicio) * 1000
                log.warning(
                    "Resuscripción sin ACK",
                    extra={
                        "sym": sym,
                        "attempt": intentos,
                        "elapsed_ms": round(elapsed_ms, 1),
                        "err": str(e),
                    },
                )
                if intentos >= max_intentos:
                    raise
                # Backoff exponencial simple con tope 5s
                espera = min(2 ** (intentos - 1), 5)
                await asyncio.sleep(espera + random.random() * 0.25)


def _es_ack(raw: str, req_id: int) -> bool:
    """Verifica que ``raw`` sea un ACK de Binance para ``req_id``."""
    try:
        data = json.loads(raw)
    except Exception:
        return False
    if data.get("id") != req_id:
        return False
    if data.get("error"):
        log.warning(f"ACK con error para id={req_id}: {data.get('error')}")
        return False
    return data.get("result") is None


async def _rellenar_gaps(
    emisor,
    symbol: str,
    ultimo_ts: int | None,
    ultimo_cierre: float | None,
    nuevo_ts: int,
    intervalo_ms: int,
):
    """Genera velas sintéticas para cubrir huecos entre ``ultimo_ts`` y ``nuevo_ts``."""
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
                'synthetic': True,
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
    backpressure: bool = False,
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

        # Emitimos la vela actual; cuando hay backpressure no aplicamos timeout
        try:
            if backpressure:
                await h['callback'](vela)
            else:
                await asyncio.wait_for(h['callback'](vela), timeout=callback_timeout)
        except asyncio.TimeoutError:
            log.warning(f'⏱️ Callback timeout ({callback_timeout}s) en {symbol}')
            registro_metrico.registrar('callback_timeouts_total', {'symbol': symbol})
            queue.task_done()
            continue
        except Exception as e:
            log.warning(f'❌ Callback falló: {e}')
            registro_metrico.registrar('callback_errors_total', {'symbol': symbol})
            queue.task_done()
            continue

        # Actualizamos estado y latidos (incluye tick_data por símbolo)
        h['ultimo_timestamp'] = vela['timestamp']
        h['ultimo_cierre'] = vela['close']
        last_message[symbol] = time.monotonic()
        tick('data_feed')
        tick_data(symbol)  # latido por símbolo en cada vela
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
    primera_vez = True
    while True:
        try:
            if primera_vez:
                await asyncio.sleep(random.random())  # jitter inicial
                primera_vez = False

            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=OPEN_TIMEOUT,
                    close_timeout=CLOSE_TIMEOUT,
                    ping_interval=None if USE_INTERNAL_KEEPALIVE else ping_interval,
                    ping_timeout=None if USE_INTERNAL_KEEPALIVE else PING_TIMEOUT,
                    max_size=2 ** 20,
                    max_queue=0,
                    ssl=_SSL_CONTEXT,
                ),
                timeout=OPEN_TIMEOUT + 5,
            )
            log.info(
                f"🔌 WebSocket conectado a {url} a las {datetime.now(UTC).isoformat()}"
            )
            _habilitar_tcp_keepalive(ws)
            fallos_consecutivos = 0

            try:
                await _resuscribir(ws, handlers)
            except Exception as e:  # noqa: PERF203 - logging
                log.warning(f"❌ Falló resuscripción inicial: {e}")
                await ws.close()
                raise

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
            last_warn: dict[str, float] = {}

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
                _procesar_cola(
                    message_queue,
                    handlers,
                    last_message,
                    handlers_by_norm,
                    backpressure=backpressure,
                )
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
                                    if faltan > MAX_BACKFILL_CANDLES:
                                        log.warning(
                                            f'Gap grande en backfill {symbol}: faltan={faltan}, limitando a {MAX_BACKFILL_CANDLES}'
                                        )
                                        registro_metrico.registrar(
                                            'backfill_gap_grande_total',
                                            {'symbol': symbol, 'faltan': int(faltan)},
                                        )
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
                            if not backpressure:
                                await asyncio.sleep(0.02)  # micro-backoff para drenar
                            symbol = _symbol_from_msg(msg)
                            if symbol:
                                ahora = time.monotonic()
                                ultimo = last_warn.get(symbol, 0.0)
                                if ahora - ultimo >= QUEUE_WARN_INTERVAL:
                                    log.warning(
                                        f"[{symbol}] queue_size={qsize}/{message_queue.maxsize}"
                                    )
                                    last_warn[symbol] = ahora
                        symbol = symbol or _symbol_from_msg(msg)
                        if symbol:
                            last_message[symbol] = time.monotonic()
                        try:
                            if backpressure:
                                # Backpressure REAL: bloquear hasta que la cola acepte
                                await message_queue.put(msg)  # sin timeout
                            else:
                                await asyncio.wait_for(message_queue.put(msg), timeout=1.0)
                        except (asyncio.TimeoutError, asyncio.QueueFull):
                            # solo cae aquí si backpressure=False
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
                        log.warning(
                            f'⏰ Sin datos en {mensaje_timeout}s, aplicando backoff y reconexión'
                        )
                        raise InactividadTimeoutError(
                            f'Sin datos en {mensaje_timeout}s'
                        )
                    except ConnectionClosed as e:
                        if 'ping timeout' in (e.reason or '').lower():
                            registro_metrico.registrar(
                                'ws_ping_timeouts_total',
                                {'url': url},
                            )
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
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        log.debug(f'Error al esperar tarea cancelada: {e}')
                        tick('data_feed')
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
            planned = min(BACKOFF_BASE * (2 ** fallos_consecutivos), MAX_BACKOFF)
            log.error(f'❌ Error en WebSocket: {e}')
            log.info(
                f'🔁 Reintentando conexión en {planned:.1f} segundos... (total reintentos: {total_reintentos})'
            )
            _registrar_reconexion()
            tick('data_feed')
            await backoff_sleep(fallos_consecutivos, base=BACKOFF_BASE, cap=MAX_BACKOFF)
            if planned >= MAX_BACKOFF and fallos_consecutivos > 1:
                log.warning(f'⚠️ Backoff máximo alcanzado: {MAX_BACKOFF}s')
            elif fallos_consecutivos >= 5:
                log.warning(
                    f'⏳ {fallos_consecutivos} fallos consecutivos. Nuevo backoff: {planned:.1f}s'
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
        tiempo_maximo = _calc_inactividad_timeout(intervalo_a_segundos(intervalo))
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
            log.info('⌛ Timeout de inactividad extendido a 300s para stream combinado')
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
        intervalo = max(1.0, min(5.0, float(tiempo_maximo) / 5.0))
        while True:
            await asyncio.sleep(intervalo)

            ahora_mono = time.monotonic()
            ultimo = last_message.get(symbol)

            if ultimo is None:
                last_message[symbol] = ahora_mono
                continue

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
                registro_metrico.registrar('ws_watchdog_timeouts_total', {'symbol': symbol})
                registro_metrico.registrar('ws_watchdog_closures_total', {'symbol': symbol})
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


async def _keepalive(ws, symbol, intervalo=PING_INTERVAL, log_interval=10):
    """Envía ping periódicamente para mantener viva la conexión."""
    if not USE_INTERNAL_KEEPALIVE:
        return
    contador = 0
    try:
        while True:
            await asyncio.sleep(intervalo)
            try:
                contador += 1
                log.debug(f'🏓 Enviando ping a {symbol}')
                inicio = time.perf_counter()
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT)
                rtt = (time.perf_counter() - inicio) * 1000
                log.debug(f'🏓 Pong recibido de {symbol} ({rtt:.1f} ms)')
                registrar_ping(symbol, rtt)
                if contador % log_interval == 0:
                    log.info(f'📡 RTT ping {symbol}: {rtt:.1f} ms')
            except asyncio.TimeoutError:
                log.warning(f'❌ Ping timeout para {symbol}')
                registro_metrico.registrar('ws_ping_timeouts_total', {'symbol': symbol})
                await ws.close()
                tick('data_feed')
                break
            except ConnectionClosed as e:
                log.info(f'🔚 WS cerrado durante ping ({symbol}) — Código: {e.code}, Motivo: {e.reason}')
                tick('data_feed')
                break
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

