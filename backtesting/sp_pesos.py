import os
import json
import optuna
import pandas as pd
import asyncio
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from optuna.pruners import MedianPruner

from core.trader_simulado import TraderSimulado
from core.logger import configurar_logger
from core.pesos import gestor_pesos

# ------------------- CONFIGURACIÓN -------------------
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
RUTA_HISTORICOS = "datos"
CARPETA_ESTRATEGIAS = "estrategias_entrada"
N_TRIALS = 60
N_BLOQUES = 5
CAPITAL_MINIMO = 970

log = configurar_logger("opt_pesos_spark")

spark = SparkSession.builder \
    .appName("OptPesosSpark") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ------------------- DETECTAR TODAS LAS ESTRATEGIAS -------------------
def detectar_estrategias_disponibles():
    archivos = os.listdir(CARPETA_ESTRATEGIAS)
    return [
        f.replace(".py", "") for f in archivos
        if f.endswith(".py")
        and f not in {"gestor_entradas.py", "analisis_pesos.py", "loader.py"}
        and not f.startswith("__")
    ]

# ------------------- CARGAR BLOQUES -------------------
def cargar_bloques(symbol, n_bloques=N_BLOQUES):
    ruta = os.path.join(RUTA_HISTORICOS, f"{symbol.replace('/', '_').lower()}_1m.parquet")
    df = spark.read.parquet(ruta).dropna().orderBy("timestamp")
    df = df.withColumn("bloque_id", monotonically_increasing_id() % n_bloques)
    bloques = [df.filter(col("bloque_id") == i).toPandas().reset_index(drop=True) for i in range(n_bloques)]
    return bloques

# ------------------- PROCESAR BLOQUE ASYNC -------------------
async def procesar_bloque(bot, symbol, bloque, barra=None):
    for fila in bloque.itertuples():
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

def simular_bloque(bloque, symbol, pesos, barra=None, capital_minimo=None):
    config_simulada = {
        "peso_minimo_total": 0.0,
        "diversidad_minima": 0,
        "cooldown_tras_perdida": 0,
        "riesgo_maximo_diario": 0.99
    }

    bot = TraderSimulado(
        [symbol],
        pesos_personalizados={symbol: pesos},
        configuraciones={symbol: config_simulada},
        modo_optimizacion=True
    )
    try:
        asyncio.run(procesar_bloque(bot, symbol, bloque, barra))
    except Exception as e:
        log.warning(f"⚠️ Error procesando bloque en {symbol}: {e}")
    capital_final = bot.capital_simulado[symbol]
    if capital_minimo and capital_final < capital_minimo:
        raise optuna.TrialPruned()
    return capital_final

# ------------------- OPTIMIZAR UN SÍMBOLO -------------------
def optimizar_pesos(symbol):
    print(f"\n🚀 Iniciando optimización para {symbol}")
    estrategias = detectar_estrategias_disponibles()
    bloques = cargar_bloques(symbol)
    velas_total = sum(len(b) for b in bloques)

    def objective(trial):
        pesos_trial = {estrategia: trial.suggest_float(estrategia, 0.0, 10) for estrategia in estrategias}
        capitales = []

        with tqdm(total=velas_total, desc=f"[{symbol}] Trial #{trial.number}", leave=False, dynamic_ncols=True) as barra:
            for bloque in bloques:
                capital = simular_bloque(bloque, symbol, pesos_trial, barra, capital_minimo=CAPITAL_MINIMO)
                capitales.append(capital)
                promedio = sum(capitales) / len(capitales)
                if promedio < CAPITAL_MINIMO:
                    raise optuna.TrialPruned()

        return sum(capitales) / len(capitales)

    study = optuna.create_study(direction="maximize", pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=2))
    study.optimize(objective, n_trials=N_TRIALS)

    print(f"✅ Finalizado {symbol} — Mejor capital: {study.best_value:.2f}")
    return study.best_params

# ------------------- MAIN -------------------
def main():
    pesos_finales = {}
    for symbol in SIMBOLOS:
        pesos_optimos = optimizar_pesos(symbol)
        pesos_finales[symbol] = pesos_optimos

    gestor_pesos.guardar(pesos_finales)
    print("\n✅ Optimización completada. Pesos guardados en config/estrategias_pesos.json")

if __name__ == "__main__":
    main()


