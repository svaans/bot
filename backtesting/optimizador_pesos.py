import asyncio
import json
import time
from typing import Dict, Iterable

import numpy as np
import optuna
import pandas as pd

from backtesting.backtest import BacktestTrader
from core.config_manager import Config
from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias

BUFFER_INICIAL = 30
N_TRIALS = 50
N_JOBS_POR_SIMBOLO = 4


def listar_estrategias_disponibles() -> list[str]:
    """Devuelve los nombres de estrategias de entrada disponibles."""
    estrategias = cargar_estrategias()
    return list(estrategias.keys())


def cargar_historicos(symbols: Iterable[str], ruta: str = "datos") -> Dict[str, pd.DataFrame]:
    datos = {}
    for s in symbols:
        archivo = f"{ruta}/{s.replace('/', '_').lower()}_1m.parquet"
        df = (
            pd.read_parquet(archivo)
            .dropna()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        datos[s] = df
    return datos


async def simular(datos: Dict[str, pd.DataFrame], pesos: Dict[str, Dict[str, float]]) -> BacktestTrader:
    config = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=list(datos.keys()),
        umbral_riesgo_diario=3.0,
        min_order_eur=10.0,
        persistencia_minima=1,
        peso_extra_persistencia=0.5,
    )
    bot = BacktestTrader(config)
    for s in config.symbols:
        bot.pesos_por_simbolo[s] = pesos.get(s, {})

    max_len = max(len(df) for df in datos.values())
    for i in range(BUFFER_INICIAL, max_len):
        tareas = []
        for symbol, df in datos.items():
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

    for symbol, df in datos.items():
        orden = bot.orders.obtener(symbol)
        if orden:
            precio_final = float(df["close"].iloc[-1])
            await bot._cerrar_y_reportar(orden, precio_final, "Fin backtest")

    return bot


def evaluar(bot: BacktestTrader) -> dict:
    capital_final = sum(bot.capital_por_simbolo.values())
    ganancias = []
    for ops in bot.resultados.values():
        ganancias.extend(ops)
    retorno_neto = capital_final - 300.0
    sharpe = 0.0
    if ganancias:
        arr = np.array(ganancias)
        if arr.std() != 0:
            sharpe = arr.mean() / arr.std() * np.sqrt(len(arr))
    return {
        "final_capital": capital_final,
        "net_return": retorno_neto,
        "sharpe": sharpe,
    }


def optimizar_pesos_symbol(symbol: str, df: pd.DataFrame, estrategias: list[str]) -> tuple[dict, dict]:
    def objective(trial: optuna.Trial) -> float:
        pesos = {e: trial.suggest_float(e, 0.1, 5.0) for e in estrategias}
        bot = asyncio.run(simular({symbol: df}, {symbol: pesos}))
        metricas = evaluar(bot)
        trial.set_user_attr("metricas", metricas)
        return metricas["final_capital"]

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=N_TRIALS, n_jobs=N_JOBS_POR_SIMBOLO)
    return study.best_params, study.best_trial.user_attrs.get("metricas", {})


def optimizar(symbols: Iterable[str]) -> Dict[str, dict]:
    estrategias = listar_estrategias_disponibles()
    datos = cargar_historicos(symbols)
    resultados = {}
    for symbol in symbols:
        print(f"\n🔧 Optimizando pesos para {symbol}...")
        best_pesos, metricas = optimizar_pesos_symbol(symbol, datos[symbol], estrategias)
        resultados[symbol] = best_pesos
        print(json.dumps(best_pesos, indent=4))
        print(json.dumps(metricas, indent=4))
    return resultados


def guardar_pesos(pesos_optimos: Dict[str, Dict[str, float]], ruta: str = "config/estrategias_pesos.json") -> None:
    gestor_pesos.guardar(pesos_optimos)
    print(f"Pesos optimizados guardados en {ruta}")


if __name__ == "__main__":
    SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
    inicio = time.time()
    nuevos_pesos = optimizar(SIMBOLOS)
    guardar_pesos(nuevos_pesos)
    duracion = round(time.time() - inicio, 2)
    print(f"Tiempo total de optimizacion: {duracion} segundos")