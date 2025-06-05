import os
import json
import optuna
import pandas as pd
import multiprocessing
import asyncio
from tqdm import tqdm
from multiprocessing import Process, Manager

from core.trader_simulado import TraderSimulado
from core.logger import configurar_logger
from core.pesos import gestor_pesos

# -------- CONFIGURACIÓN --------
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
RUTA_DATOS = "datos"
CARPETA_ESTRATEGIAS = "estrategias_entrada"
RUTA_PESOS = "config/estrategias_pesos.json"
N_TRIALS = 60
HILOS_TOTALES = max(1, multiprocessing.cpu_count() - 2)
N_JOBS_OPTUNA = max(1, HILOS_TOTALES // len(SIMBOLOS))

log = configurar_logger("opt_pesos")
log.setLevel("WARNING")  # Puedes subirlo a INFO si necesitas más trazabilidad






# -------- DETECTAR TODAS LAS ESTRATEGIAS --------
def detectar_estrategias_disponibles():
    archivos = os.listdir(CARPETA_ESTRATEGIAS)
    return [
        f.replace(".py", "") for f in archivos
        if f.endswith(".py")
        and f not in {"gestor_entradas.py", "analisis_pesos.py", "loader.py"}
        and not f.startswith("__")
    ]


# -------- CARGAR HISTÓRICO COMPLETO EN BLOQUES --------
def cargar_bloques_pandas(symbol, tam_bloque=10000):
    ruta = os.path.join(RUTA_DATOS, f"{symbol.replace('/', '_').lower()}_1m.parquet")
    df = pd.read_parquet(ruta).dropna().sort_values("timestamp").reset_index(drop=True)
    bloques = [
        (i, df.iloc[i:i+tam_bloque]) for i in range(0, len(df), tam_bloque)
        if len(df.iloc[i:i+tam_bloque]) >= 30
    ]
    return bloques


# -------- PROCESAR BLOQUES USANDO ASYNCIO --------
async def procesar_bloques_async(bot, symbol, bloques):
    for _, bloque in bloques:
        for i in range(30, len(bloque)):
            fila = bloque.iloc[i]
            vela = {
                "symbol": symbol,
                "timestamp": fila["timestamp"],
                "open": fila["open"],
                "high": fila["high"],
                "low": fila["low"],
                "close": fila["close"],
                "volume": fila["volume"],
            }
            await bot.procesar_vela(vela)


# -------- EVALUAR UN TRIAL --------
def evaluar_trial(symbol, pesos_trial, bloques):
    try:
        pesos = {symbol: pesos_trial}
        bot = TraderSimulado([symbol], pesos_personalizados=pesos)
        asyncio.run(procesar_bloques_async(bot, symbol, bloques))
        return bot.capital_simulado[symbol]
    except Exception as e:
        log.error(f"❌ Error en {symbol}: {e}", exc_info=True)
        return 0.0


# -------- FUNCIÓN OBJETIVO DE OPTUNA --------
def objetivo(trial, symbol, estrategias, bloques):
    pesos_trial = {
        estrategia: trial.suggest_float(estrategia, 0.0, 1.0)
        for estrategia in estrategias
    }
    log.info(f"🧪 [{symbol}] Trial #{trial.number} pesos: {json.dumps(pesos_trial)}")
    capital = evaluar_trial(symbol, pesos_trial, bloques)
    log.info(f"✅ [{symbol}] Trial #{trial.number} capital final: {capital:.2f}")
    return capital


# -------- OPTIMIZAR UN SÍMBOLO (PROCESO INDEPENDIENTE) --------
def optimizar_y_guardar(symbol, retorno_compartido):
    estrategias = detectar_estrategias_disponibles()
    bloques = cargar_bloques_pandas(symbol)

    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        study_name=f"opt_pesos_{symbol.replace('/', '_')}",
        storage=None
    )

    resultados_capital = []
    compras_reales = 0

    with tqdm(
        total=N_TRIALS,
        desc=f"🔄 {symbol}",
        position=SIMBOLOS.index(symbol),
        leave=True,
        dynamic_ncols=True
    ) as pbar:

        def callback(trial, state):
            pbar.update(1)

        def objetivo_local(trial):
            pesos_trial = {
                estrategia: trial.suggest_float(estrategia, 0.0, 1.0)
                for estrategia in estrategias
            }
            capital = evaluar_trial(symbol, pesos_trial, bloques)
            resultados_capital.append(capital)
            pbar.update(1)

            if capital > 1000.0:
                nonlocal compras_reales
                compras_reales += 1

            return capital

        study.optimize(
            objetivo_local,
            n_trials=N_TRIALS,
            n_jobs=N_JOBS_OPTUNA,
        )

        pbar.close()

    # ─── RESUMEN FINAL ───────────────────────────────────────────────
    promedio = sum(resultados_capital) / len(resultados_capital) if resultados_capital else 0
    minimo = min(resultados_capital) if resultados_capital else 0
    maximo = max(resultados_capital) if resultados_capital else 0

    tqdm.write(f"\n📊 [{symbol}] Resumen:")
    tqdm.write(f"   📈 Mejor capital: {study.best_value:.2f}")
    tqdm.write(f"   💰 Capital medio: {promedio:.2f}")
    tqdm.write(f"   🔻 Capital mínimo: {minimo:.2f}")
    tqdm.write(f"   🔺 Capital máximo: {maximo:.2f}")
    tqdm.write(f"   🟢 Trials con compras reales: {compras_reales}/{N_TRIALS}")


# -------- GUARDAR LOS PESOS ÓPTIMOS --------
def guardar_pesos_estrategias(pesos):
    gestor_pesos.guardar(pesos)


# -------- MAIN FINAL --------
def main():
    print(f"🧠 Núcleos disponibles: {multiprocessing.cpu_count()}, Hilos por símbolo: {N_JOBS_OPTUNA}")

    with Manager() as manager:
        retorno_compartido = manager.dict()
        procesos = []

        for symbol in SIMBOLOS:
            p = Process(target=optimizar_y_guardar, args=(symbol, retorno_compartido))
            p.start()
            procesos.append(p)

        for p in procesos:
            p.join()

        guardar_pesos_estrategias(dict(retorno_compartido))

        print("\n============================")
        print("✅ Optimización completada:")
        for symbol, pesos in retorno_compartido.items():
            print(f"🔹 {symbol}: {json.dumps(pesos, indent=2)}")
        print("============================\n")


if __name__ == "__main__":
    main()














