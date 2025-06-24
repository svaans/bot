import os
import glob
from dotenv import dotenv_values
from learning.analisis_resultados import analizar_estrategias_en_ordenes
from learning.entrenador_estrategias import normalizar_scores
from core.strategies.ajustador_pesos import ajustar_pesos_por_desempeno
from core.strategies.pesos import gestor_pesos

CONFIG = dotenv_values("config/claves.env")
MODO_REAL = CONFIG.get("MODO_REAL", "False") == "True"
CARPETA_ORDENES = "ordenes_reales" if MODO_REAL else "ordenes_simuladas"
RUTA_PESOS = "config/estrategias_pesos.json"

def recalibrar_pesos_semana() -> None:
    archivos = glob.glob(os.path.join(CARPETA_ORDENES, "*.parquet"))
    if not archivos:
        print(f"⚠️ No se encontraron archivos de órdenes en {CARPETA_ORDENES}")
        return

    resultados = {}
    for ruta in archivos:
        symbol = os.path.splitext(os.path.basename(ruta))[0].replace("_", "/")
        df_metricas = analizar_estrategias_en_ordenes(ruta)
        if df_metricas.empty:
            continue
        valores = dict(zip(df_metricas["estrategia"], df_metricas["retorno_total"]))
        resultados[symbol] = valores

    if not resultados:
        print("⚠️ Sin métricas válidas para recalibrar.")
        return

    temp_path = RUTA_PESOS + ".tmp"
    pesos_crudos = ajustar_pesos_por_desempeno(resultados, temp_path)

    pesos_normalizados = {symbol: normalizar_scores(data) for symbol, data in pesos_crudos.items()}
    gestor_pesos.guardar(pesos_normalizados)

    if os.path.exists(temp_path):
        os.remove(temp_path)
    print("✅ Pesos recalibrados correctamente.")

if __name__ == "__main__":
    recalibrar_pesos_semana()