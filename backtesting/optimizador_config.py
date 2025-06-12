import asyncio
import json
from typing import Dict, Iterable

import numpy as np
import optuna
import pandas as pd

from backtesting.backtest import BacktestTrader
from core.config_manager import Config


BUFFER_INICIAL = 30


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


async def simular(datos: Dict[str, pd.DataFrame], params: dict) -> BacktestTrader:
    config = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=list(datos.keys()),
        umbral_riesgo_diario=params["umbral_riesgo_diario"],
        min_order_eur=10.0,
        persistencia_minima=params["persistencia_minima"],
        peso_extra_persistencia=params["peso_extra_persistencia"],
    )
    bot = BacktestTrader(config)

    conf_symbol = {
        "factor_umbral": params["factor_umbral"],
        "sl_ratio": params["sl_ratio"],
        "tp_ratio": params["tp_ratio"],
        "cooldown_tras_perdida": params["cooldown_tras_perdida"],
        "riesgo_maximo_diario": params["umbral_riesgo_diario"],
    }
    for s in config.symbols:
        bot.config_por_simbolo[s] = conf_symbol.copy()

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
    retorno_neto = capital_final - 300.0  # CAPITAL_INICIAL por símbolo=100
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


def objective(trial: optuna.Trial, datos: Dict[str, pd.DataFrame]) -> float:
    params = {
        "factor_umbral": trial.suggest_float("factor_umbral", 0.5, 3.0),
        "sl_ratio": trial.suggest_float("sl_ratio", 0.5, 5.0),
        "tp_ratio": trial.suggest_float("tp_ratio", 0.5, 10.0),
        "cooldown_tras_perdida": trial.suggest_int("cooldown_tras_perdida", 0, 10),
        "persistencia_minima": trial.suggest_int("persistencia_minima", 1, 5),
        "peso_extra_persistencia": trial.suggest_float("peso_extra_persistencia", 0.0, 3.0),
        "umbral_riesgo_diario": trial.suggest_float("umbral_riesgo_diario", 1.0, 10.0),
    }
    bot = asyncio.run(simular(datos, params))
    metricas = evaluar(bot)
    trial.set_user_attr("metricas", metricas)
    return metricas["final_capital"]


def optimizar(symbols: Iterable[str]) -> tuple[dict, dict]:
    datos = cargar_historicos(symbols)
    estudio = optuna.create_study(direction="maximize")
    estudio.optimize(lambda t: objective(t, datos), n_trials=100, n_jobs=4)
    mejores_parametros = estudio.best_params
    metricas = estudio.best_trial.user_attrs.get("metricas", {})
    return mejores_parametros, metricas


def guardar_config(mejores: dict, ruta: str = "config/configuraciones_optimas.json", symbols: Iterable[str] | None = None) -> None:
    if symbols is None:
        symbols = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
    try:
        with open(ruta, "r") as f:
            datos = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        datos = {}
    for s in symbols:
        datos[s] = {
            "factor_umbral": mejores["factor_umbral"],
            "sl_ratio": mejores["sl_ratio"],
            "tp_ratio": mejores["tp_ratio"],
            "cooldown_tras_perdida": mejores["cooldown_tras_perdida"],
            "persistencia_minima": mejores["persistencia_minima"],
            "peso_extra_persistencia": mejores["peso_extra_persistencia"],
            "riesgo_maximo_diario": mejores["umbral_riesgo_diario"],
        }
    with open(ruta, "w") as f:
        json.dump(datos, f, indent=4)


if __name__ == "__main__":
    symbols = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
    mejores, metricas = optimizar(symbols)
    print("\nMejor configuración:")
    print(json.dumps(mejores, indent=4))
    print("\nMétricas:")
    print(json.dumps(metricas, indent=4))
    guardar_config(mejores, symbols=symbols)
