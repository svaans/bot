# op.py — Optimizador usando Trader modular real
import asyncio
import json
import os
import time
from typing import Dict, Iterable

import numpy as np
import optuna
import pandas as pd

from core.config_manager import Config
from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias
from core.trader_modular import Trader

BUFFER_INICIAL = 30
N_TRIALS = 60
N_JOBS = 4
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]


def cargar_historicos(symbols: Iterable[str], ruta: str = "datos") -> Dict[str, pd.DataFrame]:
    datos = {}
    for s in symbols:
        archivo = f"{ruta}/{s.replace('/', '_').lower()}_1m.parquet"
        if not os.path.isfile(archivo):
            print(f"\u26a0\ufe0f Archivo no encontrado para {s}. Se omite.")
            continue
        df = (
            pd.read_parquet(archivo)
            .dropna()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        datos[s] = df
    return datos


async def simular(df: pd.DataFrame, symbol: str, config_symbol: dict, pesos: dict) -> Trader:
    config = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=[symbol],
        umbral_riesgo_diario=config_symbol["riesgo_maximo_diario"],
        min_order_eur=10.0,
        persistencia_minima=config_symbol["persistencia_minima"],
        peso_extra_persistencia=config_symbol["peso_extra_persistencia"],
    )
    bot = Trader(config)
    bot.pesos_por_simbolo[symbol] = pesos
    bot.config_por_simbolo[symbol] = config_symbol.copy()

    for i in range(BUFFER_INICIAL, len(df)):
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
        await bot._procesar_vela(vela)

    orden = bot.orders.obtener(symbol)
    if orden:
        precio_final = float(df["close"].iloc[-1])
        await bot._cerrar_y_reportar(orden, precio_final, "Fin backtest")

    return bot


def evaluar(bot: Trader, symbol: str) -> dict:
    historial = bot.orders.historial.get(symbol, [])
    capital = bot.capital_por_simbolo.get(symbol, 0)
    if not historial:
        return {
            "capital": capital,
            "ganancia": 0,
            "winrate": 0,
            "mejor": 0,
            "peor": 0,
            "num_ops": 0,
            "sharpe": 0
        }
    resultados = np.array([op["retorno_total"] for op in historial])
    ganancias = resultados[resultados > 0]
    perdidas = resultados[resultados <= 0]
    sharpe = resultados.mean() / resultados.std() * np.sqrt(len(resultados)) if resultados.std() > 0 else 0
    return {
        "capital": capital,
        "ganancia": capital - 100,
        "winrate": len(ganancias) / len(resultados) * 100,
        "mejor": resultados.max(),
        "peor": resultados.min(),
        "num_ops": len(resultados),
        "sharpe": sharpe
    }


def optimizar_symbol(symbol: str, df: pd.DataFrame, estrategias: list[str]) -> tuple[dict, dict, dict]:
    def objective(trial: optuna.Trial) -> float:
        config_symbol = {
            "factor_umbral": trial.suggest_float("factor_umbral", 0.5, 3.0),
            "ajuste_volatilidad": trial.suggest_float("ajuste_volatilidad", 0.5, 2.0),
            "riesgo_maximo_diario": trial.suggest_float("riesgo_maximo_diario", 1.0, 10.0),
            "ponderar_por_diversidad": True,
            "modo_agresivo": False,
            "multiplicador_estrategias_recurrentes": 1.5,
            "peso_minimo_total": 2.0,
            "diversidad_minima": 2,
            "cooldown_tras_perdida": trial.suggest_int("cooldown_tras_perdida", 0, 10),
            "sl_ratio": trial.suggest_float("sl_ratio", 0.5, 5.0),
            "tp_ratio": trial.suggest_float("tp_ratio", 0.5, 10.0),
            "ratio_minimo_beneficio": 1.3,
            "persistencia_minima": trial.suggest_int("persistencia_minima", 1, 5),
            "peso_extra_persistencia": trial.suggest_float("peso_extra_persistencia", 0.0, 3.0),
        }
        pesos = {e: trial.suggest_float(e, 0.1, 5.0) for e in estrategias}

        bot = asyncio.run(simular(df, symbol, config_symbol, pesos))
        metricas = evaluar(bot, symbol)
        trial.set_user_attr("metricas", metricas)
        return metricas["capital"]

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=N_TRIALS, n_jobs=N_JOBS)

    return study.best_params, study.best_trial.user_attrs["metricas"], study.best_params


def guardar_resultados(configs: dict, pesos: dict, metricas: dict):
    with open("configuracion_y_pesos_optimos.json", "w") as f:
        json.dump({s: {"config": configs[s], "pesos": pesos[s]} for s in configs}, f, indent=4)

    with open("informe_resultados.txt", "w") as f:
        for s in metricas:
            m = metricas[s]
            f.write(f"\n📊 {s}\n")
            f.write(f"Ganancia: {m['ganancia']:.2f}\n")
            f.write(f"Winrate: {m['winrate']:.2f}%\n")
            f.write(f"N° operaciones: {m['num_ops']}\n")
            f.write(f"Mejor: {m['mejor']:.2f} | Peor: {m['peor']:.2f}\n")
            f.write(f"Sharpe Ratio: {m['sharpe']:.2f}\n")


if __name__ == "__main__":
    datos = cargar_historicos(SIMBOLOS)
    if not datos:
        raise SystemExit("No se encontraron datos de historicos para los simbolos especificados")
    estrategias = cargar_estrategias().keys()
    configuraciones, pesos_optimos, metricas_finales = {}, {}, {}
    for symbol, df in datos.items():
        print(f"\n🔍 Optimizando {symbol}...")
        config, metricas, pesos = optimizar_symbol(symbol, df, list(estrategias))
        configuraciones[symbol] = config
        pesos_optimos[symbol] = {k: v for k, v in pesos.items() if k in estrategias}
        metricas_finales[symbol] = metricas

    guardar_resultados(configuraciones, pesos_optimos, metricas_finales)
    print("\n✅ Optimización completa. Revisa 'configuracion_y_pesos_optimos.json' y 'informe_resultados.txt'")
