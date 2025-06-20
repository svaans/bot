# -*- coding: utf-8 -*-
"""Backtesting usando el Trader modular con la lógica exacta del bot en producción."""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, Dict

import pandas as pd
from tqdm import tqdm

from core.config_manager import Config
from core.trader_modular import Trader

BUFFER_INICIAL = 120
CAPITAL_INICIAL = 300.0  # Capital inicial para el backtest


class DummyCliente:
    def fetch_balance(self):
        return {"total": {"EUR": CAPITAL_INICIAL}}


class DummyRisk:
    def riesgo_superado(self, capital_total: float) -> bool:
        return False

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        pass


class BacktestTrader(Trader):
    """Versión fiel de ``Trader`` para backtesting."""

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

    async def _cerrar_y_reportar(self, orden, precio: float, motivo: str, **kwargs) -> None:
        retorno_total = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        cerrado = await self.orders.cerrar_async(orden.symbol, precio, motivo)
        if not cerrado:
            return False
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        self.capital_por_simbolo[orden.symbol] = capital_inicial + ganancia
        self.resultados[orden.symbol].append(ganancia)
        self.historial_cierres[orden.symbol] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "motivo": motivo.lower().strip(),
            "precio": precio,
            "tendencia": None,
        }
        return True


async def backtest_modular(
    symbols: Iterable[str],
    ruta_datos: str = "datos",
    fecha_inicio: datetime | None = None,
    fecha_fin: datetime | None = None,
) -> BacktestTrader:
    symbols = list(symbols)
    datos: Dict[str, pd.DataFrame] = {}
    for s in symbols:
        archivo = f"{ruta_datos}/{s.replace('/', '_').lower()}_1m.parquet"
        if not os.path.isfile(archivo):
            print(f"⚠️ Datos no encontrados para {s}. Se omite.")
            continue
        df = pd.read_parquet(archivo).dropna().sort_values("timestamp")
        if fecha_inicio is not None:
            df = df[df["timestamp"] >= pd.Timestamp(fecha_inicio)]
        if fecha_fin is not None:
            df = df[df["timestamp"] <= pd.Timestamp(fecha_fin)]
        datos[s] = df.reset_index(drop=True)

    if not datos:
        raise FileNotFoundError("No se encontraron datos de históricos para los símbolos especificados")
    
    config = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=list(datos.keys()),
        umbral_riesgo_diario=0.03,
        min_order_eur=10.0,
        persistencia_minima=1,
    )

    bot = BacktestTrader(config)
    await bot._precargar_historico(velas=BUFFER_INICIAL)

    total_ticks: Dict[str, pd.DataFrame] = datos

    max_len = max(len(df) for df in total_ticks.values())
    total_steps = sum(min(max_len, len(df)) for df in total_ticks.values())

    with tqdm(total=total_steps, desc="⏳ Procesando velas") as bar:
        for i in range(BUFFER_INICIAL, max_len):
            print(f"\n🟡 Iteración {i}/{max_len}")  # <-- NUEVO: progreso crudo
            tareas = []
            for symbol in bot.config.symbols:
                df = total_ticks[symbol]
                if i >= len(df):
                    continue
                row = df.iloc[i]
                print(f"📊 Procesando vela {symbol} | Timestamp: {row['timestamp']}")  # <-- NUEVO: símbolo y tiempo

                vela = {
                    "symbol": symbol,
                    "timestamp": row["timestamp"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                }

                fecha = pd.to_datetime(vela["timestamp"]).date()
                if fecha != bot.fecha_actual:
                    print(f"📅 Cambio de día detectado: {bot.fecha_actual} → {fecha}")
                    bot.ajustar_capital_diario(fecha=fecha)

                tareas.append(bot._procesar_vela(vela))

            if tareas:
                try:
                    await asyncio.wait_for(asyncio.gather(*tareas), timeout=5.0)
                except asyncio.TimeoutError:
                    print("⏱️ Tarea bloqueada por más de 5 segundos. Posible cuelgue detectado.")
                    continue
                print(f"✅ Iteración {i} completada con {len(tareas)} velas")  # <-- NUEVO
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
    symbols = sys.argv[1:] or ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
    asyncio.run(backtest_modular(symbols))
