import os
import json
import optuna
import pandas as pd
import asyncio
from tqdm import tqdm
from optuna.pruners import MedianPruner
from core.trader_simulado import TraderSimulado
from core.logger import configurar_logger
from core.pesos import gestor_pesos
from core.configuracion import guardar_configuracion_simbolo

# ------------------- CONFIGURACIÓN -------------------
SIMBOLOS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]
RUTA_DATOS = "datos"
CARPETA_ESTRATEGIAS = "estrategias_entrada"
N_TRIALS = 30
CAPITAL_MINIMO = 970
N_BLOQUES = 3
N_JOBS_OPTUNA = max(1, os.cpu_count() - 1)
log = configurar_logger("opt_conjunto")

# ------------------- DETECTAR TODAS LAS ESTRATEGIAS -------------------
def detectar_estrategias_disponibles():
    archivos = os.listdir(CARPETA_ESTRATEGIAS)
    return [
        f.replace(".py", "") for f in archivos
        if f.endswith(".py") and
        f not in {"gestor_entradas.py", "analisis_pesos.py", "loader.py"} and
        not f.startswith("__")
    ]

# ------------------- CARGAR BLOQUES -------------------
def cargar_bloques(symbol, n_bloques=N_BLOQUES):
    ruta = os.path.join(RUTA_DATOS, f"{symbol.replace('/', '_').lower()}_1m.parquet")
    df = pd.read_parquet(ruta).dropna().sort_values("timestamp").reset_index(drop=True)
    bloques = [
        df.iloc[i::n_bloques].reset_index(drop=True)
        for i in range(n_bloques)
        if len(df.iloc[i::n_bloques]) > 30
    ]
    return bloques

# ------------------- PROCESAR BLOQUE -------------------
async def procesar_bloques(bot, symbol, bloques, barra=None):
    for bloque in bloques:
        for fila in bloque.itertuples():
            vela = {
                "symbol": symbol,
                "timestamp": fila.timestamp,
                "open": fila.open,
                "high": fila.high,
                "low": fila.low,
                "close": fila.close,
                "volume": fila.volume,
            }
            await bot.procesar_vela(vela)
            if barra:
                barra.update(1)

def simular_bloques(bloques, symbol, pesos, config, barra=None, capital_minimo=None):
    bot = TraderSimulado(
        [symbol],
        pesos_personalizados={symbol: pesos},
        configuraciones={symbol: config},
        modo_optimizacion=True,
    )
    try:
        asyncio.run(procesar_bloques(bot, symbol, bloques, barra))
    except Exception as e:
        log.warning(f"⚠️ Error procesando bloques en {symbol}: {e}")
    capital_final = bot.capital_simulado[symbol]
    if capital_minimo and capital_final < capital_minimo:
        raise optuna.TrialPruned()
    return capital_final

# ------------------- OPTIMIZAR CONFIG + PESOS -------------------
def optimizar_todo(symbol):
    print(f"\n🚀 Optimización conjunta para {symbol}")
    estrategias = detectar_estrategias_disponibles()
    bloques = cargar_bloques(symbol)
    total_velas = sum(len(b) for b in bloques)

    def objective(trial):
        # Configuración
        config = {
            "factor_umbral": trial.suggest_float("factor_umbral", 0.8, 2.5),
            "ajuste_volatilidad": trial.suggest_float("ajuste_volatilidad", 0.8, 2.0),
            "ponderar_por_diversidad": trial.suggest_categorical("ponderar_por_diversidad", [True, False]),
            "modo_agresivo": trial.suggest_categorical("modo_agresivo", [True, False]),
            "multiplicador_estrategias_recurrentes": trial.suggest_float("multiplicador_estrategias_recurrentes", 0.5, 2.0),
            "peso_minimo_total": trial.suggest_float("peso_minimo_total", 0.0, 5.0),
            "diversidad_minima": trial.suggest_int("diversidad_minima", 0, 5),
            "cooldown_tras_perdida": trial.suggest_int("cooldown_tras_perdida", 0, 15),
            "sl_ratio": trial.suggest_float("sl_ratio", 1.0, 6.0),
            "tp_ratio": trial.suggest_float("tp_ratio", 1.0, 6.0),
            "riesgo_maximo_diario": trial.suggest_float("riesgo_maximo_diario", 1.0, 3.0)
        }

        # Pesos
        pesos = {estrategia: trial.suggest_float(estrategia, 0.0, 10.0) for estrategia in estrategias}
        

        with tqdm(total=total_velas, desc=f"[{symbol}] Trial {trial.number}", leave=False, dynamic_ncols=True) as barra:
            capital = simular_bloques(bloques, symbol, pesos, config, barra, capital_minimo=CAPITAL_MINIMO)

        return capital

    study = optuna.create_study(direction="maximize", pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=2))
    study.optimize(objective, n_trials=N_TRIALS, n_jobs=N_JOBS_OPTUNA)

    print(f"✅ Finalizado {symbol} — Mejor capital promedio: {study.best_value:.2f}")

    # Separar pesos y config
    params = study.best_params
    config_final = {k: v for k, v in params.items() if k in {
        "factor_umbral",
        "ajuste_volatilidad",
        "ponderar_por_diversidad",
        "modo_agresivo",
        "multiplicador_estrategias_recurrentes",
        "peso_minimo_total",
        "diversidad_minima",
        "cooldown_tras_perdida",
        "sl_ratio",
        "tp_ratio",
        "riesgo_maximo_diario",
    }}
    pesos_finales = {k: v for k, v in params.items() if k not in config_final}

    guardar_configuracion_simbolo(symbol, config_final)
    return symbol, pesos_finales

# ------------------- MAIN -------------------
def main():
    pesos_por_simbolo = {}
    for symbol in SIMBOLOS:
        symbol, pesos = optimizar_todo(symbol)
        pesos_por_simbolo[symbol] = pesos

    gestor_pesos.guardar(pesos_por_simbolo)
    print("\n✅ Optimización conjunta completada. Pesos y configuración guardados.")

if __name__ == "__main__":
    main()
