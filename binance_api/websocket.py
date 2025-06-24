import asyncio
import json
import websockets
import traceback
from core.utils.utils import configurar_logger

log = configurar_logger("websocket")

def normalizar_symbolo(symbol):
    return symbol.replace("/", "").lower()

async def escuchar_velas(symbol, intervalo, callback):
    url = f"wss://stream.binance.com:9443/ws/{normalizar_symbolo(symbol)}@kline_{intervalo}"
    intentos = 0

    while True:
        try:
            ws = await websockets.connect(url, ping_interval=20, ping_timeout=20)
            log.info(f"🔌 WebSocket conectado para {symbol} ({intervalo})")
            intentos = 0  # reiniciar intentos al conectar
            try:

                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        vela = data["k"]
                        if vela["x"]:
                            log.info(
                                f"\n✅ Vela cerrada de {symbol} — Close: {vela['c']}, Vol: {vela['v']}"
                            )
                            await callback(
                                {
                                    "symbol": symbol,
                                    "timestamp": vela["t"],
                                    "open": float(vela["o"]),
                                    "high": float(vela["h"]),
                                    "low": float(vela["l"]),
                                    "close": float(vela["c"]),
                                    "volume": float(vela["v"]),
                                }
                            )

                    except asyncio.CancelledError:
                        log.info(f"🛑 WebSocket de {symbol} cancelado (salida ordenada).")
                        raise
                    except Exception as e:
                        log.warning(f"❌ Error procesando mensaje de {symbol}: {e}")
                        traceback.print_exc()
            finally:
                try:
                    await ws.close()
                    await ws.wait_closed()
                except Exception:
                    pass

        except asyncio.CancelledError:
            log.info(f"🛑 Conexión WebSocket de {symbol} cancelada.")
            break

        except Exception as e:
            intentos += 1
            espera = min(60, 5 * intentos)  # Espera progresiva hasta 1 min
            log.warning(f"❌ Error en WebSocket de {symbol}: {e}")
            traceback.print_exc()
            log.info(f"🔁 Reintentando conexión en {espera} segundos...")
            await asyncio.sleep(espera)
