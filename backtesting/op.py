# optimizador_completo.py
import asyncio
import json
import os
import time
import random
from typing import Dict, Iterable

import numpy as np
import optuna
import pandas as pd
from optuna.exceptions import TrialPruned

from backtesting.backtest import BacktestTrader
from core.config_manager import Config
from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias

BUFFER_INICIAL = 30
N_TRIALS = 60
N_JOBS = 4
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
VELAS_POR_DIA = 1440
DIAS_BLOQUE = 3


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


def seleccionar_bloque(df: pd.DataFrame) -> pd.DataFrame:
    total_velas = len(df)
    bloque_size = VELAS_POR_DIA * DIAS_BLOQUE
    if total_velas <= bloque_size:
        return df.copy()
    inicio = random.randint(0, total_velas - bloque_size)
    return df.iloc[inicio:inicio + bloque_size].reset_index(drop=True)


async def simular(df: pd.DataFrame, symbol: str, config_symbol: dict, pesos: dict) -> BacktestTrader:
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
    bot = BacktestTrader(config)
    bot.pesos_por_simbolo[symbol] = pesos
    bot.config_por_simbolo[symbol] = config_symbol.copy()

    resultados_temporales = []
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

        # Pruning: tras 30% del bloque, abortar si hay perdidas
        if i == int(len(df) * 0.3):
            resultados = bot.resultados.get(symbol, [])
            if resultados and sum(resultados) < 0:
                raise TrialPruned("🔪 Ganancia negativa al 30% del trial")

        # Pruning por desempeño temprano
        resultados = bot.resultados.get(symbol, [])
        if len(resultados) >= 10:
            ganancias = [r for r in resultados if r > 0]
            winrate = len(ganancias) / len(resultados)
            if np.mean(resultados) < 0 and winrate < 0.4:
                raise TrialPruned("🔪 Rendimiento inicial deficiente")

    orden = bot.orders.obtener(symbol)
    if orden:
        precio_final = float(df["close"].iloc[-1])
        await bot._cerrar_y_reportar(orden, precio_final, "Fin backtest")

    return bot


def evaluar(bot: BacktestTrader, symbol: str) -> dict:
    resultados = bot.resultados.get(symbol, [])
    capital = bot.capital_por_simbolo.get(symbol, 0)
    if not resultados:
        return {
            "capital": capital,
            "ganancia": 0,
            "winrate": 0,
            "mejor": 0,
            "peor": 0,
            "num_ops": 0,
            "sharpe": 0
        }
    arr = np.array(resultados)
    ganancias = arr[arr > 0]
    perdidas = arr[arr <= 0]
    sharpe = arr.mean() / arr.std() * np.sqrt(len(arr)) if arr.std() > 0 else 0
    return {
        "capital": capital,
        "ganancia": capital - 100,
        "winrate": len(ganancias) / len(arr) * 100,
        "mejor": arr.max(),
        "peor": arr.min(),
        "num_ops": len(arr),
        "sharpe": sharpe
    }


def optimizar_symbol(symbol: str, df: pd.DataFrame, estrategias: list[str]) -> tuple[dict, dict, dict]:
    bloque_df = seleccionar_bloque(df)

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

        inicio = time.time()
        bot = asyncio.run(simular(bloque_df, symbol, config_symbol, pesos))
        if time.time() - inicio > 60:
            raise TrialPruned("⏱️ Trial excedió el límite de tiempo")

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
    estrategias = cargar_estrategias().keys()
    configuraciones, pesos_optimos, metricas_finales = {}, {}, {}
    for symbol in SIMBOLOS:
        print(f"\n🔍 Optimizando {symbol}...")
        config, metricas, pesos = optimizar_symbol(symbol, datos[symbol], list(estrategias))
        configuraciones[symbol] = config
        pesos_optimos[symbol] = {k: v for k, v in pesos.items() if k in estrategias}
        metricas_finales[symbol] = metricas

    guardar_resultados(configuraciones, pesos_optimos, metricas_finales)
    print("\n✅ Optimización completa. Revisa 'configuracion_y_pesos_optimos.json' y 'informe_resultados.txt'")
