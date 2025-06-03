import os
import json
import optuna
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, row_number, to_date
from pyspark.sql.window import Window
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import asyncio

from core.trader_simulado import TraderSimulado
from core.logger import configurar_logger
from core.pesos import cargar_pesos_estrategias
from core.configuracion import guardar_configuracion_simbolo

# -------- CONFIGURACIÓN --------
RUTA_DATOS = "datos"
RUTA_CONFIGS = "config/configuraciones_optimas.json"
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
N_TRIALS = 60
N_JOBS = 3
N_JOBS_OPTUNA = 4

log = configurar_logger("opt_config")
spark = SparkSession.builder.appName("OptimizadorConfigSpark").getOrCreate()


# -------- BLOQUES CON SPARK --------
def cargar_historico_bloques(symbol, tamano_bloque=10000):
    ruta = os.path.join(RUTA_DATOS, f"{symbol.replace('/', '_').lower()}_1m.parquet")
    df = spark.read.parquet(ruta).dropna().withColumnRenamed("timestamp", "date")
    df = df.withColumn("fecha", to_date("date"))
    window = Window.partitionBy("fecha").orderBy("date")
    df = df.withColumn("row_num", row_number().over(window))
    df = df.withColumn("bloque_id", floor((col("row_num") - 1) / tamano_bloque))
    bloque_ids = [row["bloque_id"] for row in df.select("bloque_id").distinct().collect()]
    bloques = [(i, df.filter(col("bloque_id") == i).drop("row_num", "bloque_id")) for i in bloque_ids]
    return bloques


# -------- EVALUACIÓN DE BLOQUES --------
async def evaluar_configuracion(symbol, config, bloques, pesos_symbol):
    capital_total = 0.0
    for bloque_id, bloque_df in bloques:
        pdf = bloque_df.toPandas()
        if len(pdf) < 30:
            continue

        bot = TraderSimulado(
            [symbol],
            configuraciones={symbol: config},
            pesos_personalizados={symbol: pesos_symbol}
        )

        for i in range(30, len(pdf)):
            fila = pdf.iloc[i]
            vela = {
                "symbol": symbol,
                "timestamp": fila["date"],
                "open": fila["open"],
                "high": fila["high"],
                "low": fila["low"],
                "close": fila["close"],
                "volume": fila["volume"],
            }
            await bot.procesar_vela(vela)

        capital_total += bot.capital_simulado[symbol] - 1000.0

    return 1000.0 + capital_total


# -------- OBJETIVO DE OPTIMIZACIÓN --------
def objective(trial, symbol, bloques, pesos_symbol):
    config = {
        "factor_umbral": trial.suggest_float("factor_umbral", 0.8, 2.0),
        "ajuste_volatilidad": trial.suggest_float("ajuste_volatilidad", 0.5, 2.0),
        "riesgo_maximo_diario": trial.suggest_float("riesgo_maximo_diario", 0.05, 0.35),
        "ponderar_por_diversidad": trial.suggest_categorical("ponderar_por_diversidad", [True, False]),
        "modo_agresivo": trial.suggest_categorical("modo_agresivo", [True, False]),
        "multiplicador_estrategias_recurrentes": trial.suggest_float("multiplicador_estrategias_recurrentes", 0.3, 2.0),
        "peso_minimo_total": trial.suggest_float("peso_minimo_total", 0.1, 1.5),
        "diversidad_minima": trial.suggest_int("diversidad_minima", 1, 3),
        "cooldown_tras_perdida": trial.suggest_int("cooldown_tras_perdida", 0, 20),
        "sl_ratio": trial.suggest_float("sl_ratio", 0.01, 0.05),
        "tp_ratio": trial.suggest_float("tp_ratio", 0.03, 0.10),
    }

    log.info(f"🧪 [{symbol}] Trial #{trial.number} comenzando con config: {json.dumps(config)}")

    try:
        capital = asyncio.run(evaluar_configuracion(symbol, config, bloques, pesos_symbol))
        log.info(f"✅ [{symbol}] Trial #{trial.number} | Capital: {capital:.2f}")
        return capital
    except Exception as e:
        log.error(f"❌ [{symbol}] Error en trial #{trial.number}: {e}", exc_info=True)
        return 0


# -------- OPTIMIZACIÓN POR SÍMBOLO --------
def optimizar_config_symbolo(symbol):
    bloques = cargar_historico_bloques(symbol)
    pesos = cargar_pesos_estrategias()
    pesos_symbol = pesos.get(symbol, {})

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        study_name=f"opt_config_{symbol.replace('/', '_')}",
        storage="sqlite:///optuna_config.db",
        load_if_exists=True
    )

    def wrapper(trial):
        return objective(trial, symbol, bloques, pesos_symbol)

    with tqdm(total=N_TRIALS, desc=f"🔧 {symbol}") as pbar:
        def callback(trial, state):
            pbar.update(1)

        study.optimize(wrapper, n_trials=N_TRIALS, n_jobs=N_JOBS_OPTUNA, callbacks=[callback])

    mejor_config = study.best_params
    log.info(f"🏆 [{symbol}] Mejor capital: {study.best_value:.2f}")
    return symbol, mejor_config


# -------- GUARDAR CONFIGURACIONES --------
def guardar_configuraciones(configs):
    with open(RUTA_CONFIGS, "w") as f:
        json.dump(configs, f, indent=4)
    log.info(f"✅ Configuraciones guardadas en {RUTA_CONFIGS}")


# -------- EJECUCIÓN PRINCIPAL --------
def main():
    resultados = {}
    with ThreadPoolExecutor(max_workers=N_JOBS) as executor:
        futuros = [executor.submit(optimizar_config_symbolo, s) for s in SIMBOLOS]
        for f in as_completed(futuros):
            symbol, config = f.result()
            resultados[symbol] = config

    guardar_configuraciones(resultados)

    print("\n===========================")
    print("✅ Optimización de CONFIGURACIONES completada:")
    for s, c in resultados.items():
        print(f"🔹 {s}: {json.dumps(c, indent=2)}")
    print("===========================\n")


if __name__ == "__main__":
    main()





