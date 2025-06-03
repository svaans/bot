import os
import json
import pandas as pd
from datetime import datetime
from .analisis_resultados import analizar_estrategias_en_ordenes
from core.ajustador_pesos import ajustar_pesos_por_desempeno
from core.pesos import cargar_pesos_estrategias, guardar_pesos_estrategias
from core.adaptador_umbral import calcular_umbral_adaptativo
from dotenv import dotenv_values

# Configuración global
CONFIG = dotenv_values("claves.env")
MODO_REAL = CONFIG.get("MODO_REAL", "False") == "True"
CARPETA_ORDENES = "ordenes_reales" if MODO_REAL else "ordenes_simuladas"
RUTA_PESOS = "config/estrategias_pesos.json"




def registrar_resultado_trade(orden: dict):
    """
    Guarda la orden ejecutada (real o simulada) y actualiza pesos de estrategias en caliente.
    """
    symbol = orden.get("symbol")
    if not symbol or "estrategias_activas" not in orden:
        print("⚠️ Orden incompleta, no se puede registrar.")
        return

    timestamp = orden.get("timestamp", datetime.utcnow().timestamp())
    orden["timestamp"] = timestamp

    # Convertir a DataFrame para análisis incremental
    df_orden = pd.DataFrame([orden])

    # Ruta del archivo donde se acumulan todas las órdenes (por modo)
    ruta_archivo = f"{CARPETA_ORDENES}/{symbol.replace('/', '_')}.json"

    if os.path.exists(ruta_archivo):
        df_existente = pd.read_json(ruta_archivo)
        df_ordenes = pd.concat([df_existente, df_orden], ignore_index=True)
    else:
        df_ordenes = df_orden

    df_ordenes.to_json(ruta_archivo, orient="records", indent=2)
    print(f"📝 Orden guardada en {ruta_archivo}")

    # Aprendizaje rápido (solo si hay al menos 20 operaciones para evitar sobreajuste)
    if len(df_ordenes) >= 20:
        df_metricas = analizar_estrategias_en_ordenes(ruta_archivo)
        pesos_actuales = cargar_pesos_estrategias().get(symbol, {})
        if pesos_actuales:
            nuevos_pesos = ajustar_pesos_por_desempeno(df_metricas, pesos_actuales)
            guardar_pesos_estrategias({symbol: nuevos_pesos})
            print("✅ Pesos actualizados tras operación.")





