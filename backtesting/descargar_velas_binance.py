import ccxt
import pandas as pd
import time
import os
from datetime import timedelta

def descargar_historico(symbol="ETH/EUR", filename="eth_eur_1m.parquet", timeframe="1m", dias_historico=30):
    exchange = ccxt.binance()
    exchange.load_markets()

    tf_minutos = int(timeframe[:-1]) if timeframe.endswith("m") else 15
    velas_por_dia = int(24 * 60 / tf_minutos)
    max_barras = velas_por_dia * dias_historico
    limite = 1000

    all_data = []
    since = exchange.milliseconds() - (max_barras * tf_minutos * 60 * 1000)

    print(f"⏳ Descargando {dias_historico} días de datos ({max_barras} velas) para {symbol}...")

    while len(all_data) < max_barras:
        try:
            data = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limite)
        except Exception as e:
            print(f"⚠️ Error al pedir datos: {e}")
            time.sleep(5)
            continue

        if not data:
            break

        all_data += data
        since = data[-1][0] + 1
        print(f"📊 Velas acumuladas: {len(all_data)}")
        time.sleep(exchange.rateLimit / 1000)

        if len(data) < limite:
            break

    df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ruta_guardado = os.path.join(BASE_DIR, "..", "datos")  # Guardar en carpeta datos
    os.makedirs(ruta_guardado, exist_ok=True)

    ruta_final = os.path.join(ruta_guardado, filename)
    df.to_parquet(ruta_final, index=False)

    print(f"✅ Archivo guardado en {ruta_final} con {len(df)} velas.\n")

if __name__ == "__main__":
    DIAS_HISTORICO = 10
    descargar_historico("BTC/EUR", "btc_eur_1m.parquet", dias_historico=DIAS_HISTORICO)
    descargar_historico("ETH/EUR", "eth_eur_1m.parquet", dias_historico=DIAS_HISTORICO)
    descargar_historico("ADA/EUR", "ada_eur_1m.parquet", dias_historico=DIAS_HISTORICO)
    descargar_historico("SOL/EUR", "sol_eur_1m.parquet", dias_historico=DIAS_HISTORICO)
    descargar_historico("BNB/EUR", "bnb_eur_1m.parquet", dias_historico=DIAS_HISTORICO)
