import os
import json
import pandas as pd
from datetime import datetime
from .analisis_resultados import analizar_estrategias_en_ordenes
from core.ajustador_pesos import ajustar_pesos_por_desempeno
from core.pesos import gestor_pesos
from core.adaptador_dinamico import calcular_umbral_adaptativo
from dotenv import dotenv_values

# Configuración global
CONFIG = dotenv_values("config/claves.env")
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
    ruta_archivo = f"{CARPETA_ORDENES}/{symbol.replace('/', '_')}.parquet"

    if os.path.exists(ruta_archivo):
        df_existente = pd.read_parquet(ruta_archivo)
        df_ordenes = pd.concat([df_existente, df_orden], ignore_index=True)
    else:
        df_ordenes = df_orden

    df_ordenes.to_parquet(ruta_archivo, index=False)
    print(f"📝 Orden guardada en {ruta_archivo}")

    # Aprendizaje rápido (solo si hay al menos 20 operaciones para evitar sobreajuste)
    if len(df_ordenes) >= 20:
        df_metricas = analizar_estrategias_en_ordenes(ruta_archivo)
        if not df_metricas.empty:
            valores = dict(zip(df_metricas["estrategia"], df_metricas["retorno_total"]))
            temp_path = RUTA_PESOS + ".tmp"
            calculados = ajustar_pesos_por_desempeno({symbol: valores}, temp_path)
            nuevos_pesos = calculados.get(symbol, {})

            if os.path.exists(temp_path):
                os.remove(temp_path)

            if nuevos_pesos:
                datos = gestor_pesos.pesos
                pesos_previos = datos.get(symbol, {})
                pesos_previos.update(nuevos_pesos)
                datos[symbol] = pesos_previos
                gestor_pesos.guardar(datos)
                print("✅ Pesos actualizados tras operación.")





