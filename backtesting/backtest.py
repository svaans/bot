import os
import pandas as pd
import asyncio
from datetime import datetime
from tqdm import tqdm
from typing import Iterable, Dict
from core.trader_simulado import TraderSimulado

BUFFER_INICIAL = 30
CAPITAL_INICIAL = 1000.0

async def backtest_con_datos_historicos(
    symbols: Iterable[str], ruta_datos: str = "datos"
) -> TraderSimulado:
    """Ejecuta el backtesting con velas históricas para los *symbols* indicados."""

    bot = TraderSimulado(list(symbols))

    total_ticks: Dict[str, pd.DataFrame] = {
        s: pd.read_parquet(
            f"{ruta_datos}/{s.replace('/', '_').lower()}_1m.parquet"
        )
        .dropna()
        .sort_values("timestamp")
        for s in bot.symbols
    }

    max_largo = max(len(df) for df in total_ticks.values())
    total_pasos = sum(min(max_largo, len(df)) for df in total_ticks.values())
    

    with tqdm(total=total_pasos, desc="⏳ Procesando velas") as progress_bar:
        for i in range(BUFFER_INICIAL, max_largo):
            tareas = []
            for symbol in bot.symbols:
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

                tareas.append(bot.procesar_vela(vela))

            if tareas:
                await asyncio.gather(*tareas)
                progress_bar.update(len(tareas))

    await bot.cerrar()
    generar_informe_completo(bot)
    return bot

def generar_informe_completo(
    bot: TraderSimulado, capital_inicial: float = CAPITAL_INICIAL
) -> None:
    """Genera un informe de resultados en consola y en `resultados/`."""

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
        rentabilidad = ganancia / capital_inicial * 100

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
    rentabilidad_total = (total_ganancia / (capital_inicial * len(bot.symbols))) * 100

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


