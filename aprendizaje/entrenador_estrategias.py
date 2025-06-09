import os
import json
import time
import pandas as pd
from collections import defaultdict
from dotenv import dotenv_values
from core.pesos import gestor_pesos
from core.logger import configurar_logger

# Cargar configuración del .env
CONFIG = dotenv_values("config/claves.env")
MODO_REAL = CONFIG.get("MODO_REAL", "False") == "True"
CARPETA_ORDENES = "ordenes_reales" if MODO_REAL else "ordenes_simuladas"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA_HISTORICO = os.path.join(BASE_DIR, "ultimas_operaciones")
RUTA_PESOS = "config/estrategias_pesos.json"
MIN_OPERACIONES = 5

log = configurar_logger("trader_simulado", modo_silencioso=True)

def evaluar_estrategias(ordenes: pd.DataFrame):
    datos = defaultdict(list)
    for _, orden in ordenes.iterrows():
        estrategias = orden.get("estrategias_activas", {})
        if isinstance(estrategias, str):
            try:
                estrategias = json.loads(estrategias.replace("'", "\""))
            except Exception as e:
                print(f"⚠️ Error al parsear estrategias: {e}")
                continue
        retorno = orden.get("retorno_total", 0.0)
        for estrategia, activa in estrategias.items():
            if activa:
                datos[estrategia].append(retorno)
    return datos

def normalizar_scores(scores):
    max_score = max(scores.values(), default=1)
    return {k: v / max_score for k, v in scores.items()}

def dividir_train_test(df: pd.DataFrame, test_ratio: float = 0.2):
    """Divide el DataFrame en particiones de entrenamiento y prueba."""
    if df.empty:
        return df, pd.DataFrame()
    n_test = max(1, int(len(df) * test_ratio))
    df_train = df.iloc[:-n_test]
    df_test = df.iloc[-n_test:]
    return df_train, df_test

def actualizar_pesos_estrategias_symbol(symbol: str):
    FACTOR_SUAVIZADO = 0.02  # 2% de ajuste diario máximo

    archivo = f"{symbol.replace('/', '_').upper()}.parquet"
    ruta = os.path.join(CARPETA_HISTORICO, archivo)

    for intento in range(3):
        if os.path.exists(ruta):
            break
        time.sleep(0.3)

    if not os.path.exists(ruta):
        print(f"⚠️ No se encontró historial para {symbol} en {CARPETA_HISTORICO}")
        return

    try:
        ordenes = pd.read_parquet(ruta)
    except Exception as e:
        print(f"❌ Error al leer el archivo {ruta}: {e}")
        return

    if len(ordenes) < MIN_OPERACIONES:
        log.info(f"⚠️ Insuficientes operaciones para {symbol}.")
        return

    train_df, test_df = dividir_train_test(ordenes)

    datos_estrategias = evaluar_estrategias(train_df)
    nuevos_scores = {}

    for estrategia, retornos in datos_estrategias.items():
        if len(retornos) < MIN_OPERACIONES:
            continue
        promedio = sum(retornos) / len(retornos)
        winrate = sum(1 for r in retornos if r > 0) / len(retornos)
        score = promedio * winrate * 100
        if score > 0:
            nuevos_scores[estrategia] = score

    if not nuevos_scores:
        log.info(f"⚠️ No se generaron scores válidos para {symbol}.")
        return

    # Normalización de los nuevos scores (entre 0 y 1)
    nuevos_scores_normalizados = normalizar_scores(nuevos_scores)

    # Cargar pesos actuales
    pesos_totales = gestor_pesos.pesos
    pesos_actuales = pesos_totales.get(symbol, {})

    # Aplicar suavizado
    pesos_suavizados = {}
    for estrategia, score in nuevos_scores_normalizados.items():
        peso_actual = pesos_actuales.get(estrategia, 0.5)
        peso_nuevo = peso_actual * (1 - FACTOR_SUAVIZADO) + score * FACTOR_SUAVIZADO
        pesos_suavizados[estrategia] = peso_nuevo

    # Guardar nuevos pesos
    pesos_totales[symbol] = pesos_suavizados
    gestor_pesos.guardar(pesos_totales)

    print(f"✅ Pesos suavizados para {symbol} en modo {'REAL' if MODO_REAL else 'SIMULADO'}:")
    for estrategia, peso in pesos_suavizados.items():
        print(f"  - {estrategia}: {peso:.3f}")

    # --------- Validación básica usando el conjunto de prueba ---------
    datos_test = evaluar_estrategias(test_df)
    resultados = [r for lst in datos_test.values() for r in lst]
    if resultados:
        promedio = sum(resultados) / len(resultados)
        winrate = sum(1 for r in resultados if r > 0) / len(resultados)
        log.info(
            f"📊 Validación {symbol}: retorno medio {promedio:.3f}, winrate {winrate*100:.2f}%"
        )
