import asyncio
import json
import websockets
import traceback

def normalizar_symbolo(symbol):
    return symbol.replace("/", "").lower()

async def escuchar_velas(symbol, intervalo, callback):
    url = f"wss://stream.binance.com:9443/ws/{normalizar_symbolo(symbol)}@kline_{intervalo}"
    intentos = 0

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                print(f"🔌 WebSocket conectado para {symbol} ({intervalo})")
                intentos = 0  # reiniciar intentos al conectar

                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        vela = data["k"]
                        if vela["x"]:
                            print(f"\n✅ Vela cerrada de {symbol} — Close: {vela['c']}, Vol: {vela['v']}")
                            await callback({
                                "symbol": symbol,
                                "timestamp": vela["t"],
                                "open": float(vela["o"]),
                                "high": float(vela["h"]),
                                "low": float(vela["l"]),
                                "close": float(vela["c"]),
                                "volume": float(vela["v"])
                            })

                    except asyncio.CancelledError:
                        print(f"🛑 WebSocket de {symbol} cancelado (salida ordenada).")
                        raise
                    except Exception as e:
                        print(f"❌ Error procesando mensaje de {symbol}: {e}")
                        traceback.print_exc()

        except asyncio.CancelledError:
            print(f"🛑 Conexión WebSocket de {symbol} cancelada.")
            break

        except Exception as e:
            intentos += 1
            espera = min(60, 5 * intentos)  # Espera progresiva hasta 1 min
            print(f"❌ Error en WebSocket de {symbol}: {e}")
            traceback.print_exc()
            print(f"🔁 Reintentando conexión en {espera} segundos...")
            await asyncio.sleep(espera)
