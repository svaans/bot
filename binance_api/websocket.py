import asyncio
import json
import traceback
from datetime import datetime
from collections import deque
import socket

import websockets
from websockets.exceptions import ConnectionClosed

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
_reconnect_history: deque[datetime] = deque()
MAX_BACKOFF = 60
MAX_BACKFILL_CANDLES = 100  # límite para backfill para evitar saturación


def _registrar_reconexion() -> None:
    """Registra un intento de reconexión y alerta si la tasa es elevada."""
    ahora = datetime.utcnow()
    _reconnect_history.append(ahora)
    while _reconnect_history and (ahora - _reconnect_history[0]).total_seconds() > RECONNECT_WINDOW:
        _reconnect_history.popleft()
    if len(_reconnect_history) > RECONNECT_THRESHOLD:
        log.warning(
            f'⚠️ Tasa de reconexión alta: {len(_reconnect_history)} en {RECONNECT_WINDOW // 60} min'
        )



def normalizar_symbolo(symbol: str) ->str:
    log.info('➡️ Entrando en normalizar_symbolo()')
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


async def escuchar_velas(
    symbol: str,
    intervalo: str,
    callback,
    last_message: dict[str, datetime] | None = None,
    tiempo_maximo: int | None = None,
    ping_interval: int | None = None,
    cliente=None,
    mensaje_timeout: int | None = None,
):
    """Escucha velas cerradas de ``symbol`` en ``intervalo``.

    Si ``cliente`` se proporciona, al reconectar se intentará recuperar
    posibles velas perdidas usando :func:`fetch_ohlcv_async`.
    """
    log.debug('➡️ Entrando en escuchar_velas()')
    """
    Conecta al websocket de Binance para recibir velas cerradas y
    llamar al callback con los datos normalizados.

    :param symbol: str, ejemplo "BTC/USDT"
    :param intervalo: str, ejemplo "1m"
    :param callback: función async para procesar la vela
    :param mensaje_timeout: segundos máximos a esperar cada mensaje
        antes de forzar reconexión. Si ``None`` se espera indefinidamente.
    """
    if not isinstance(symbol, str) or '/' not in symbol:
        raise ValueError(f'Símbolo inválido: {symbol}')
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError(f'Intervalo inválido: {intervalo}')
    url = (
        f'wss://stream.binance.com:9443/ws/{normalizar_symbolo(symbol)}@kline_{intervalo}'
        )
    if last_message is None:
        last_message = {}
    if tiempo_maximo is None:
        # Margen amplio: esperamos ~4 intervalos antes de forzar reconexión
        tiempo_maximo = max(intervalo_a_segundos(intervalo) * 4, 60)
    if ping_interval is None:
        ping_interval = 30  # ping manual para detectar antes conexiones muertas
    if mensaje_timeout is None:
        mensaje_timeout = tiempo_maximo
    fallos_consecutivos = 0
    total_reintentos = 0
    backoff = 5
    ultimo_timestamp: int | None = None
    ultimo_cierre: float | None = None
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
    while True:
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=10,
                    close_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2 ** 20,
                ),
                timeout=15,
            )
            log.info(
                f"🔌 WebSocket conectado para {symbol} ({intervalo}) a las {datetime.utcnow().isoformat()}"
            )
            _habilitar_tcp_keepalive(ws)
            fallos_consecutivos = 0
            backoff = 5
            last_message[symbol] = datetime.utcnow()
            watchdog = asyncio.create_task(
                _watchdog(ws, symbol, last_message, tiempo_maximo)
            )
            keeper = asyncio.create_task(_keepalive(ws, symbol, ping_interval))

            backfill_task = None
            if cliente and ultimo_timestamp is not None:
                async def _backfill():
                    nonlocal ultimo_timestamp, ultimo_cierre
                    intentos = 0
                    espera = 1
                    while True:
                        try:
                            ahora = int(datetime.utcnow().timestamp() * 1000)
                            faltan = max(1, (ahora - ultimo_timestamp) // intervalo_ms)
                            limite = min(faltan, MAX_BACKFILL_CANDLES)
                            ohlcv = await asyncio.wait_for(
                                fetch_ohlcv_async(
                                    cliente,
                                    symbol=symbol,
                                    timeframe=intervalo,
                                    since=ultimo_timestamp + 1,
                                    limit=limite,
                                ),
                                timeout=10,
                            )
                            for o in ohlcv:
                                ts = o[0]
                                if ts > ultimo_timestamp:
                                    ultimo_timestamp = await _rellenar_gaps(
                                        callback,
                                        symbol,
                                        ultimo_timestamp,
                                        ultimo_cierre,
                                        ts,
                                        intervalo_ms,
                                    )
                                    await callback(
                                        {
                                            'symbol': symbol,
                                            'timestamp': ts,
                                            'open': float(o[1]),
                                            'high': float(o[2]),
                                            'low': float(o[3]),
                                            'close': float(o[4]),
                                            'volume': float(o[5]),
                                        }
                                    )
                                    ultimo_timestamp = ts
                                    ultimo_cierre = float(o[4])
                            break
                        except Exception as e:
                            intentos += 1
                            if intentos >= 3:
                                log.warning(
                                    f'❌ Error al backfillear {symbol} tras {intentos} intentos: {e}'
                                )
                                tick('data_feed')
                                break
                            await asyncio.sleep(espera)
                            espera *= 2

                backfill_task = asyncio.create_task(_backfill())  # ejecútalo en paralelo
            try:
                while True:
                    try:
                        if mensaje_timeout:
                            msg = await asyncio.wait_for(ws.recv(), timeout=mensaje_timeout)
                        else:
                            msg = await ws.recv()
                        last_message[symbol] = datetime.utcnow()
                    except asyncio.TimeoutError:
                        log.warning(
                            f'⏰ Sin datos de {symbol} en {mensaje_timeout}s, forzando reconexión'
                        )
                        await ws.close()
                        break
                    except ConnectionClosed as e:
                        log.warning(
                            f"🚪 WebSocket cerrado en {symbol} — Código: {e.code}, Motivo: {e.reason}"
                        )
                        await ws.close()
                        break
                    except (ConnectionError, asyncio.TimeoutError) as e:
                        log.warning(f'❌ Conexión perdida en {symbol}: {e}')
                        await ws.close()
                        break
                    except Exception as e:
                        log.warning(f'❌ Error recibiendo datos de {symbol}: {e}')
                        await ws.close()
                        tick('data_feed')
                        raise
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError as e:
                        log.warning(
                            f'❌ Error al decodificar JSON en {symbol}: {e}')
                        continue
                    except Exception as e:
                        log.warning(
                            f'❌ Error procesando mensaje dentro del bucle {symbol}: {e}'
                            )
                        tick('data_feed')
                        continue
                    if data.get('e') != 'kline':
                        log.debug(
                            f"⚠️ Evento no esperado en {symbol}: {data.get('e')}"
                            )
                        continue
                    try:
                        vela = data['k']
                        if vela['x']:
                            log.info(
                                f"✅ Vela cerrada {symbol} — Close: {vela['c']}, Vol: {vela['v']}"
                            )
                            latencia = datetime.utcnow().timestamp() * 1000 - vela['t']
                            log.debug(
                                f"⏱️ Latencia de vela {symbol}: {latencia:.0f} ms"
                            )
                            ultimo_timestamp = await _rellenar_gaps(
                                callback,
                                symbol,
                                ultimo_timestamp,
                                ultimo_cierre,
                                vela['t'],
                                intervalo_ms,
                            )
                            await callback(
                                {
                                    'symbol': symbol,
                                    'timestamp': vela['t'],
                                    'open': float(vela['o']),
                                    'high': float(vela['h']),
                                    'low': float(vela['l']),
                                    'close': float(vela['c']),
                                    'volume': float(vela['v']),
                                }
                            )
                            ultimo_timestamp = vela['t']
                            ultimo_cierre = float(vela['c'])
                            tick('data_feed')
                    except Exception as e:
                        log.warning(f'❌ Error en callback de {symbol}: {e}')
                        tick('data_feed')
                        traceback.print_exc()
            finally:
                log.info(
                    f"🔻 WebSocket desconectado para {symbol} a las {datetime.utcnow().isoformat()}"
                )
                if backfill_task:
                    try:
                        await backfill_task
                    except Exception as e:
                        log.debug(
                            f'Error en backfill de {symbol}: {e}'
                        )
                        tick('data_feed')
                for t in (watchdog, keeper):
                    t.cancel()
                for t in (watchdog, keeper):
                    try:
                        await t
                    except InactividadTimeoutError:
                        raise
                    except Exception as e:
                        log.debug(
                            f'Error al esperar tarea cancelada de {symbol}: {e}'
                        )
                        tick('data_feed')
                try:
                    await ws.close()
                    await ws.wait_closed()
                except Exception as e:
                    log.debug(f'Error al cerrar WebSocket de {symbol}: {e}')
                    tick('data_feed')
        except asyncio.CancelledError:
            log.info(f'🛑 Conexión WebSocket de {symbol} cancelada.')
            break
        except Exception as e:
            fallos_consecutivos += 1
            total_reintentos += 1
            log.warning(f'❌ Error en WebSocket de {symbol}: {e}')
            traceback.print_exc()
            log.info(
                f'🔁 Reintentando conexión en {backoff} segundos... (total reintentos: {total_reintentos})'
            )
            _registrar_reconexion()
            tick('data_feed')
            await asyncio.sleep(backoff)
            previo = backoff
            backoff = min(MAX_BACKOFF, backoff * 2)
            if backoff == MAX_BACKOFF and previo < MAX_BACKOFF:
                log.warning(f'⚠️ Backoff máximo alcanzado: {MAX_BACKOFF}s')
            elif fallos_consecutivos >= 5:
                log.warning(
                    f'⏳ {fallos_consecutivos} fallos consecutivos. Nuevo backoff: {backoff}s'
                )


async def escuchar_velas_combinado(
    symbols: list[str],
    intervalo: str,
    handlers: dict[str, callable],
    last_message: dict[str, datetime] | None = None,
    tiempo_maximo: int | None = None,
    ping_interval: int | None = None,
    cliente=None,
    mensaje_timeout: int | None = None,
):
    """Escucha velas cerradas de múltiples símbolos usando un stream combinado.

    Cada símbolo debe contar con un handler asociado en ``handlers``. El payload
    recibido se despachará al handler según el campo ``stream`` del mensaje.
    """
    log.debug('➡️ Entrando en escuchar_velas_combinado()')
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
    fallos_consecutivos = 0
    total_reintentos = 0
    backoff = 5
    ultimo_timestamp: dict[str, int | None] = {s: None for s in symbols}
    ultimo_cierre: dict[str, float | None] = {s: None for s in symbols}
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
    while True:
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=10,
                    close_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2 ** 20,
                ),
                timeout=15,
            )
            log.info(
                f"🔌 WebSocket combinado conectado para {symbols} ({intervalo}) a las {datetime.utcnow().isoformat()}"
            )
            _habilitar_tcp_keepalive(ws)
            fallos_consecutivos = 0
            backoff = 5
            for s in symbols:
                last_message[s] = datetime.utcnow()
            # Cada símbolo posee su propio watchdog. Si uno de ellos detecta
            # inactividad forzará el cierre del WebSocket compartido para
            # reconectar el stream completo. Esta lógica simplifica la
            # reconexión, pero implica que la pérdida de datos de un par
            # provoca un reinicio global afectando temporalmente a los demás.
            watchdogs = [
                asyncio.create_task(_watchdog(ws, s, last_message, tiempo_maximo))
                for s in symbols
            ]
            keeper = asyncio.create_task(
                _keepalive(ws, 'combined', ping_interval)
            )

            backfill_tasks = []
            if cliente:
                for s in symbols:
                    ts = ultimo_timestamp.get(s)
                    if ts is None:
                        continue
                    async def _backfill_symbol(symbol=s, ts=ts):
                        intentos = 0
                        espera = 1
                        while True:
                            try:
                                ahora = int(datetime.utcnow().timestamp() * 1000)
                                faltan = max(1, (ahora - ts) // intervalo_ms)
                                limite = min(faltan, MAX_BACKFILL_CANDLES)
                                ohlcv = await asyncio.wait_for(
                                    fetch_ohlcv_async(
                                        cliente,
                                        symbol=symbol,
                                        timeframe=intervalo,
                                        since=ts + 1,
                                        limit=limite,
                                    ),
                                    timeout=10,
                                )
                                for o in ohlcv:
                                    tss = o[0]
                                    if tss > ts:
                                        uc = ultimo_cierre.get(symbol)
                                        ts = await _rellenar_gaps(
                                            handlers[symbol],
                                            symbol,
                                            ts,
                                            uc,
                                            tss,
                                            intervalo_ms,
                                        )
                                        await handlers[symbol](
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
                                        ultimo_timestamp[symbol] = tss
                                        ultimo_cierre[symbol] = float(o[4])
                                break
                            except Exception as e:
                                intentos += 1
                                if intentos >= 3:
                                    log.warning(
                                        f'❌ Error al backfillear {symbol} tras {intentos} intentos: {e}'
                                    )
                                    tick('data_feed')
                                    break
                                await asyncio.sleep(espera)
                                espera *= 2

                    backfill_tasks.append(asyncio.create_task(_backfill_symbol()))  # paralelo
            try:
                while True:
                    try:
                        if mensaje_timeout:
                            msg = await asyncio.wait_for(
                                ws.recv(), timeout=mensaje_timeout
                            )
                        else:
                            msg = await ws.recv()
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
                    except (ConnectionError, asyncio.TimeoutError) as e:
                        log.warning(f'❌ Conexión perdida: {e}')
                        await ws.close()
                        break
                    except Exception as e:
                        log.warning(f'❌ Error recibiendo datos: {e}')
                        await ws.close()
                        tick('data_feed')
                        raise
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError as e:
                        log.warning(f'❌ Error al decodificar JSON: {e}')
                        continue
                    except Exception as e:
                        log.warning(
                            f'❌ Error procesando mensaje dentro del bucle combinado: {e}'
                        )
                        tick('data_feed')
                        continue
                    stream = data.get('stream')
                    payload = data.get('data', {})
                    if not stream or payload.get('e') != 'kline':
                        log.debug(f"⚠️ Evento no esperado: {data}")
                        continue
                    sym_norm = stream.split('@')[0]
                    symbol = normalizados.get(sym_norm)
                    if not symbol:
                        log.debug(f'⚠️ Símbolo desconocido en stream {stream}')
                        continue
                    last_message[symbol] = datetime.utcnow()
                    try:
                        vela = payload['k']
                        if vela['x']:
                            log.info(
                                f"✅ Vela cerrada {symbol} — Close: {vela['c']}, Vol: {vela['v']}"
                            )
                            latencia = (
                                datetime.utcnow().timestamp() * 1000 - vela['t']
                            )
                            log.debug(
                                f"⏱️ Latencia de vela {symbol}: {latencia:.0f} ms"
                            )
                            ts_prev = ultimo_timestamp.get(symbol)
                            uc = ultimo_cierre.get(symbol)
                            ts_prev = await _rellenar_gaps(
                                handlers[symbol],
                                symbol,
                                ts_prev,
                                uc,
                                vela['t'],
                                intervalo_ms,
                            )
                            if ts_prev is not None:
                                ultimo_timestamp[symbol] = ts_prev
                            await handlers[symbol](
                                {
                                    'symbol': symbol,
                                    'timestamp': vela['t'],
                                    'open': float(vela['o']),
                                    'high': float(vela['h']),
                                    'low': float(vela['l']),
                                    'close': float(vela['c']),
                                    'volume': float(vela['v']),
                                }
                            )
                            ultimo_timestamp[symbol] = vela['t']
                            ultimo_cierre[symbol] = float(vela['c'])
                            tick('data_feed')
                    except Exception as e:
                        log.warning(f'❌ Error en callback de {symbol}: {e}')
                        tick('data_feed')
                        traceback.print_exc()
            finally:
                log.info(
                    f"🔻 WebSocket combinado desconectado para {symbols} a las {datetime.utcnow().isoformat()}"
                )
                for t in backfill_tasks:
                    try:
                        await t
                    except Exception as e:
                        log.debug(
                            f'Error en backfill del stream combinado: {e}'
                        )
                        tick('data_feed')
                for t in watchdogs + [keeper]:
                    t.cancel()
                for t in watchdogs + [keeper]:
                    try:
                        await t
                    except InactividadTimeoutError:
                        raise
                    except Exception as e:
                        log.debug(
                            f'Error al esperar tarea cancelada del stream combinado: {e}'
                        )
                        tick('data_feed')
                try:
                    await ws.close()
                    await ws.wait_closed()
                except Exception as e:
                    log.debug(
                        f'Error al cerrar WebSocket del stream combinado: {e}'
                    )
                    tick('data_feed')
        except asyncio.CancelledError:
            log.info('🛑 Conexión WebSocket combinada cancelada.')
            break
        except Exception as e:
            fallos_consecutivos += 1
            total_reintentos += 1
            log.warning(f'❌ Error en WebSocket combinado: {e}')
            traceback.print_exc()
            log.info(
                f'🔁 Reintentando conexión en {backoff} segundos... (total reintentos: {total_reintentos})'
            )
            _registrar_reconexion()
            tick('data_feed')
            await asyncio.sleep(backoff)
            previo = backoff
            backoff = min(MAX_BACKOFF, backoff * 2)
            if backoff == MAX_BACKOFF and previo < MAX_BACKOFF:
                log.warning(f'⚠️ Backoff máximo alcanzado: {MAX_BACKOFF}s')
            elif fallos_consecutivos >= 5:
                log.warning(
                    f'⏳ {fallos_consecutivos} fallos consecutivos. Nuevo backoff: {backoff}s'
                )


async def _watchdog(
    ws,
    symbol: str,
    last_message: dict[str, datetime],
    tiempo_maximo: int,
):
    """Cierra ``ws`` si no se reciben datos por ``tiempo_maximo`` segundos."""
    log.debug('➡️ Entrando en _watchdog()')
    """
    Si no llega ninguna vela en tiempo_maximo (segundos), cierra el websocket para reiniciar.
    """
    try:
        while True:
            await asyncio.sleep(tiempo_maximo)
            ultimo = last_message.get(symbol)
            if not ultimo:
                last_message[symbol] = datetime.utcnow()
                continue
            if (datetime.utcnow() - ultimo).total_seconds() > tiempo_maximo:
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
    except Exception as e:
        log.debug(f'Excepción inesperada en watchdog de {symbol}: {e}')
        tick('data_feed')
        tick_data(symbol)

async def _keepalive(ws, symbol, intervalo=30):
    """Envía ping periódicamente para mantener viva la conexión."""
    try:
        while True:
            await asyncio.sleep(intervalo)
            try:
                log.debug(f'🏓 Enviando ping a {symbol}')
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=10)
                log.debug(f'🏓 Pong recibido de {symbol}')
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
        log.debug(f'No se pudo configurar TCP keep-alive: {e}')
        tick('data_feed')
