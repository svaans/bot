# -*- coding: utf-8 -*-
"""Backtesting usando el Trader modular sin side effects."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable, Dict

import pandas as pd
from tqdm import tqdm

from core.config_manager import Config
from core.trader_modular import Trader

BUFFER_INICIAL = 30
CAPITAL_INICIAL = 1000.0  # Capital inicial para el backtest


class DummyCliente:
    def fetch_balance(self):
        return {"total": {"EUR": CAPITAL_INICIAL}}


class DummyRisk:
    def riesgo_superado(self, capital_total: float) -> bool:
        return False

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        pass


class BacktestTrader(Trader):
    """Versión simplificada de ``Trader`` para backtesting."""

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.notificador = None
        self.orders.notificador = None
        self.orders.risk = DummyRisk()
        self.cliente = DummyCliente()
        self.orders.ordenes = {}
        self.resultados: Dict[str, list] = {s: [] for s in config.symbols}
        capital_unit = CAPITAL_INICIAL / max(len(config.symbols), 1)
        capital_unit = max(capital_unit, 20.0)
        self.capital_por_simbolo = {s: capital_unit for s in config.symbols}
        self.capital_inicial_diario = self.capital_por_simbolo.copy()

    def _cerrar_y_reportar(self, orden, precio: float, motivo: str) -> None:
        retorno_total = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        self.orders.cerrar(orden.symbol, precio, motivo)
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        self.capital_por_simbolo[orden.symbol] = capital_inicial + ganancia
        self.resultados[orden.symbol].append(ganancia)
        self.historial_cierres[orden.symbol] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "motivo": motivo.lower().strip(),
        }


async def backtest_modular(symbols: Iterable[str], ruta_datos: str = "datos") -> BacktestTrader:
    """Ejecuta el backtesting utilizando el Trader modular."""

    config = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=list(symbols),
        umbral_riesgo_diario=0.03,
        min_order_eur=10.0,
        persistencia_minima=1,
    )

    bot = BacktestTrader(config)

    total_ticks: Dict[str, pd.DataFrame] = {
        s: pd.read_parquet(f"{ruta_datos}/{s.replace('/', '_').lower()}_1m.parquet")
        .dropna()
        .sort_values("timestamp")
        for s in bot.config.symbols
    }

    max_len = max(len(df) for df in total_ticks.values())
    total_steps = sum(min(max_len, len(df)) for df in total_ticks.values())

    with tqdm(total=total_steps, desc="⏳ Procesando velas") as bar:
        for i in range(BUFFER_INICIAL, max_len):
            tareas = []
            for symbol in bot.config.symbols:
                df = total_ticks[symbol]
                if i >= len(df):
                    continue
                row = df.iloc[i]
                vela = {
                    "symbol": symbol,
                    "timestamp": row["timestamp"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                }
                tareas.append(bot._procesar_vela(vela))
            if tareas:
                await asyncio.gather(*tareas)
                bar.update(len(tareas))

    await bot.cerrar()
    generar_informe(bot)
    return bot


def generar_informe(bot: BacktestTrader, capital_inicial: float = CAPITAL_INICIAL) -> None:
    print("🧾 INFORME DE BACKTESTING")
    print(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

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

        print(f"📌 {symbol}")
        print(f"- Operaciones totales: {total}")
        print(f"- Ganancia neta: {ganancia:.2f} €")
        print(f"- Rentabilidad: {rentabilidad:.2f} %")
        print(f"- Winrate: {winrate:.2f} % ({ganadoras}/{total})")
        print(f"- Mejor operación: {ganancia_max:.2f} €")
        print(f"- Peor operación: {perdida_max:.2f} €\n")

    winrate_global = (total_ganadoras / total_ordenes * 100) if total_ordenes else 0
    rentabilidad_total = (
        total_ganancia / (capital_inicial * len(bot.config.symbols)) * 100
        if bot.config.symbols
        else 0
    )

    print("🎯 RESUMEN GLOBAL")
    print(f"- Total operaciones: {total_ordenes}")
    print(f"- Winrate global: {winrate_global:.2f} %")
    print(f"- Ganancia total: {total_ganancia:.2f} €")
    print(f"- Rentabilidad total: {rentabilidad_total:.2f} %")


if __name__ == "__main__":
    import sys

    logging.disable(logging.CRITICAL)
    symbols = sys.argv[1:] or ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
    asyncio.run(backtest_modular(symbols))