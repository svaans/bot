import asyncio
import json
import traceback
from datetime import datetime

import websockets
from websockets.exceptions import ConnectionClosed

from core.utils.utils import configurar_logger, intervalo_a_segundos
# Si en el futuro se escuchan streams de profundidad (@depthUpdate),
# será necesario validar la secuencia de mensajes con el esquema
# ``U <= lastUpdateId + 1 <= u`` y obtener un snapshot inicial vía
# ``/api/v3/depth`` antes de procesar las actualizaciones.
from core.supervisor import tick, tick_data

log = configurar_logger('websocket')



def normalizar_symbolo(symbol: str) ->str:
    log.info('➡️ Entrando en normalizar_symbolo()')
    return symbol.replace('/', '').lower()


INTERVALOS_VALIDOS = {'1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'}


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
        tiempo_maximo = max(intervalo_a_segundos(intervalo) * 2, 60)
    if ping_interval is None:
        ping_interval = intervalo_a_segundos(intervalo)
    if mensaje_timeout is None:
        mensaje_timeout = tiempo_maximo
    intentos = 0
    total_reintentos = 0
    backoff = 5
    ultimo_timestamp: int | None = None
    while True:
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    open_timeout=10,
                    close_timeout=10,
                    ping_interval=ping_interval,
                    ping_timeout=None,
                    max_size=2 ** 20,
                ),
                timeout=15,
            )
            log.info(f'🔌 WebSocket conectado para {symbol} ({intervalo})')
            intentos = 0
            backoff = 5
            last_message[symbol] = datetime.utcnow()
            watchdog = asyncio.create_task(
                _watchdog(ws, symbol, last_message, tiempo_maximo)
            )
            keeper = asyncio.create_task(_keepalive(ws, symbol, ping_interval))

            if cliente and ultimo_timestamp is not None:
                try:
                    ohlcv = await fetch_ohlcv_async(
                        cliente,
                        symbol=symbol,
                        timeframe=intervalo,
                        since=ultimo_timestamp + 1,
                    )
                    for o in ohlcv:
                        ts = o[0]
                        if ts > ultimo_timestamp:
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
                except Exception as e:
                    log.warning(f'❌ Error al backfillear {symbol}: {e}')
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
                            tick('data_feed')
                    except Exception as e:
                        log.warning(f'❌ Error en callback de {symbol}: {e}')
                        traceback.print_exc()
            finally:
                for t in (watchdog, keeper):
                    t.cancel()
                    try:
                        await t
                    except Exception:
                        pass
                try:
                    await ws.close()
                    await ws.wait_closed()
                except Exception:
                    pass
        except asyncio.CancelledError:
            log.info(f'🛑 Conexión WebSocket de {symbol} cancelada.')
            break
        except Exception as e:
            intentos += 1
            total_reintentos += 1
            log.warning(f'❌ Error en WebSocket de {symbol}: {e}')
            traceback.print_exc()
            log.info(
                f'🔁 Reintentando conexión en {backoff} segundos... (total reintentos: {total_reintentos})'
            )
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)


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
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        pass

async def _keepalive(ws, symbol, intervalo=60):
    """Envía ping periódicamente para mantener viva la conexión."""
    try:
        while True:
            await asyncio.sleep(intervalo)
            try:
                log.debug(f'🏓 Enviando ping a {symbol}')
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=10)
            except Exception as e:
                log.warning(f'❌ Ping falló para {symbol}: {e}')
                await ws.close()
                break
    except asyncio.CancelledError:
        raise
