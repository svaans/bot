import os
import pandas as pd
import asyncio
from datetime import datetime
from tqdm import tqdm
from core.trader_simulado import TraderSimulado

async def backtest_con_datos_historicos(symbols, ruta_datos="datos"):
    bot = TraderSimulado(symbols)
    total_ticks = {}

    # Cargar los datos por símbolo
    for symbol in symbols:
        archivo = f"{ruta_datos}/{symbol.replace('/', '_').lower()}_1m.parquet"
        df = pd.read_parquet(archivo).dropna().sort_values("timestamp")
        total_ticks[symbol] = df

    max_largo = max(len(df) for df in total_ticks.values())
    total_pasos = sum(min(max_largo, len(df)) for df in total_ticks.values())
    progress_bar = tqdm(total=total_pasos, desc="⏳ Procesando velas")

    # Procesamiento vela a vela
    for i in range(30, max_largo):
        for symbol in symbols:
            df = total_ticks[symbol]
            if i >= len(df):
                continue

            fila = df.iloc[i]
            vela = {
                "symbol": symbol,
                "timestamp": fila["timestamp"],
                "open": fila["open"],
                "high": fila["high"],
                "low": fila["low"],
                "close": fila["close"],
                "volume": fila["volume"],
            }

            await bot.procesar_vela(vela)
            progress_bar.update(1)

    progress_bar.close()
    await bot.cerrar()
    generar_informe_completo(bot)

def generar_informe_completo(bot: TraderSimulado):
    informe = []
    informe.append("🧾 INFORME DE BACKTESTING")
    informe.append(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    informe.append("")

    total_ordenes = 0
    total_ganancia = 0.0
    total_ganadoras = 0

    for symbol, operaciones in bot.resultados.items():
        if not operaciones:
            continue
        total = len(operaciones)
        ganancia = sum(operaciones)
        ganadoras = len([x for x in operaciones if x > 0])
        perdida_max = min(operaciones)
        ganancia_max = max(operaciones)
        winrate = ganadoras / total * 100
        rentabilidad = ganancia / 1000 * 100  # capital inicial

        total_ordenes += total
        total_ganancia += ganancia
        total_ganadoras += ganadoras

        informe.append(f"📌 {symbol}")
        informe.append(f"- Operaciones totales: {total}")
        informe.append(f"- Ganancia neta: {ganancia:.2f} €")
        informe.append(f"- Rentabilidad: {rentabilidad:.2f} %")
        informe.append(f"- Winrate: {winrate:.2f} % ({ganadoras}/{total})")
        informe.append(f"- Mejor operación: {ganancia_max:.2f} €")
        informe.append(f"- Peor operación: {perdida_max:.2f} €")
        informe.append("")

    winrate_global = (total_ganadoras / total_ordenes * 100) if total_ordenes else 0
    rentabilidad_total = (total_ganancia / (1000 * len(bot.symbols))) * 100

    informe.append("🎯 RESUMEN GLOBAL")
    informe.append(f"- Total operaciones: {total_ordenes}")
    informe.append(f"- Winrate global: {winrate_global:.2f} %")
    informe.append(f"- Ganancia total: {total_ganancia:.2f} €")
    informe.append(f"- Rentabilidad total: {rentabilidad_total:.2f} %")

    # Guardar informe
    os.makedirs("resultados", exist_ok=True)
    ruta_txt = "resultados/informe_backtesting.txt"
    with open(ruta_txt, "w", encoding="utf-8") as f:
        for linea in informe:
            print(linea)
            f.write(linea + "\n")

    print(f"\n📁 Informe guardado en: {ruta_txt}")

if __name__ == "__main__":
    symbols = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
    asyncio.run(backtest_con_datos_historicos(symbols))


