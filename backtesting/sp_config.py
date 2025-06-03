import os
import json
import optuna
import pandas as pd
import asyncio
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

from core.trader_simulado import TraderSimulado
from core.logger import configurar_logger
from core.configuracion import guardar_configuracion_simbolo
from estrategias_entrada.gestor_entradas import evaluar_estrategias

# ------------------- CONFIGURACIÓN -------------------
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
RUTA_HISTORICOS = "datos"
N_TRIALS = 60
N_BLOQUES = 5

log = configurar_logger("opt_config_spark")

spark = SparkSession.builder \
    .appName("OptConfigSpark") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ------------------- CARGAR BLOQUES -------------------
def cargar_bloques(symbol, n_bloques=N_BLOQUES):
    ruta = os.path.join(RUTA_HISTORICOS, f"{symbol.replace('/', '_').lower()}_1m.parquet")
    df = spark.read.parquet(ruta).dropna().orderBy("timestamp")
    df = df.withColumn("bloque_id", monotonically_increasing_id() % n_bloques)
    bloques = [df.filter(col("bloque_id") == i).toPandas().reset_index(drop=True) for i in range(n_bloques)]
    return bloques

# ------------------- PROCESAR BLOQUE ASYNC -------------------
async def procesar_bloque(bot, symbol, bloque, barra=None):
    for idx, fila in enumerate(bloque.itertuples(), 1):
        vela = {
            "symbol": symbol,
            "timestamp": fila.timestamp,
            "open": fila.open,
            "high": fila.high,
            "low": fila.low,
            "close": fila.close,
            "volume": fila.volume
        }
        await bot.procesar_vela(vela)
        if barra:
            barra.update(1)

def simular_bloque(bloque, symbol, config, barra=None):
    bot = TraderSimulado([symbol], configuraciones={symbol: config}, modo_optimizacion=True)
    try:
        asyncio.run(procesar_bloque(bot, symbol, bloque, barra))
    except Exception as e:
        log.warning(f"⚠️ Error procesando bloque en {symbol}: {e}")
    return bot.capital_simulado[symbol]

# ------------------- OPTIMIZAR CONFIGURACIÓN -------------------
def optimizar_configuracion(symbol):
    print(f"\n🚀 Optimizando configuración para {symbol}")
    bloques = cargar_bloques(symbol)
    velas_total = sum(len(b) for b in bloques)

    def objective(trial):
        config_trial = {
            "peso_minimo_total": trial.suggest_float("peso_minimo_total", 0.1, 0.6),
            "diversidad_minima": trial.suggest_int("diversidad_minima", 1, 2),
            "cooldown_tras_perdida": trial.suggest_int("cooldown_tras_perdida", 0, 2),
            "sl_ratio": trial.suggest_float("sl_ratio", 0.005, 0.02),
            "tp_ratio": trial.suggest_float("tp_ratio", 0.01, 0.1),
            "riesgo_maximo_diario": trial.suggest_float("riesgo_maximo_diario", 0.2, 0.9),
            "ponderar_por_diversidad": trial.suggest_categorical("ponderar_por_diversidad", [False]),
            "modo_agresivo": trial.suggest_categorical("modo_agresivo", [True]),
            "multiplicador_estrategias_recurrentes": trial.suggest_float("multiplicador_estrategias_recurrentes", 1.0, 1.0),  # fijo para estabilidad
        }

        bloques = cargar_bloques(symbol)
        velas_total = sum(len(b) for b in bloques)

        with tqdm(total=velas_total, desc=f"[{symbol}] Trial #{trial.number}", leave=False, dynamic_ncols=True) as barra:
            capitales = [simular_bloque(b, symbol, config_trial, barra) for b in bloques]

        return sum(capitales) / len(capitales)

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=N_TRIALS)

    print(f"✅ Finalizado {symbol} — Mejor capital: {study.best_value:.2f}")
    return study.best_params

# ------------------- MAIN -------------------
def main():
    for symbol in SIMBOLOS:
        mejor_config = optimizar_configuracion(symbol)
        guardar_configuracion_simbolo(symbol, mejor_config)

    print("\n✅ Optimización de configuraciones completada. Guardadas en config/configuraciones_optimas.json")

if __name__ == "__main__":
    main()