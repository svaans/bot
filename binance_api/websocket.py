import asyncio
import json
import traceback
from datetime import datetime

import websockets
from core.utils.utils import configurar_logger
from core.supervisor import tick, tick_data

log = configurar_logger('websocket')

last_message: dict[str, datetime] = {}


def normalizar_symbolo(symbol: str) ->str:
    log.info('➡️ Entrando en normalizar_symbolo()')
    return symbol.replace('/', '').lower()


INTERVALOS_VALIDOS = {'1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'}


async def escuchar_velas(symbol: str, intervalo: str, callback):
    """Escucha velas cerradas de ``symbol`` en ``intervalo``."""
    log.debug('➡️ Entrando en escuchar_velas()')
    """
    Conecta al websocket de Binance para recibir velas cerradas y
    llamar al callback con los datos normalizados.

    :param symbol: str, ejemplo "BTC/USDT"
    :param intervalo: str, ejemplo "1m"
    :param callback: función async para procesar la vela
    """
    if not isinstance(symbol, str) or '/' not in symbol:
        raise ValueError(f'Símbolo inválido: {symbol}')
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError(f'Intervalo inválido: {intervalo}')
    url = (
        f'wss://stream.binance.com:9443/ws/{normalizar_symbolo(symbol)}@kline_{intervalo}'
        )
    intentos = 0
    total_reintentos = 0
    backoff = 5
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
            log.info(f'🔌 WebSocket conectado para {symbol} ({intervalo})')
            intentos = 0
            backoff = 5
            watchdog = asyncio.create_task(_watchdog(ws, symbol))
            try:
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        log.warning(f'⏰ Sin datos de {symbol} en 30s, forzando reconexión')
                        await ws.close()
                        break
                    except Exception:
                        log.warning(f'❌ Error recibiendo datos de {symbol}, reconectando')
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
                            tick('data_feed')
                            watchdog.cancel()
                            watchdog = asyncio.create_task(_watchdog(ws, symbol))
                    except Exception as e:
                        log.warning(f'❌ Error en callback de {symbol}: {e}')
                        traceback.print_exc()
            finally:
                try:
                    watchdog.cancel()
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


async def _watchdog(ws, symbol, tiempo_maximo=300):
    """Cierra ``ws`` si no se reciben velas en ``tiempo_maximo`` segundos."""
    log.debug('➡️ Entrando en _watchdog()')
    """
    Si no llega ninguna vela en tiempo_maximo (segundos), cierra el websocket para reiniciar.
    """
    try:
        await asyncio.sleep(tiempo_maximo)
        log.warning(
            f'⚠️ No se recibieron velas en {tiempo_maximo}s para {symbol}, forzando reconexión.'
            )
        await ws.close()
        tick('data_feed')
        tick_data(symbol)
    except Exception:
        pass
