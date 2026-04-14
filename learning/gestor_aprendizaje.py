import os
import json
import pandas as pd
from datetime import datetime, timezone

UTC = timezone.utc
from .analisis_resultados import analizar_estrategias_en_ordenes
from core.strategies.ajustador_pesos import ajustar_pesos_por_desempeno
from core.strategies.pesos import entry_weights_temp_path, gestor_pesos
from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights
from dotenv import dotenv_values

from .historial_operaciones import normalizar_symbol_parquet_filename

CONFIG = dotenv_values('config/claves.env')
MODO_REAL = CONFIG.get('MODO_REAL', 'False') == 'True'
CARPETA_ORDENES = 'ordenes_reales' if MODO_REAL else 'ordenes_simuladas'
def registrar_resultado_trade(orden: dict):
    """
    Guarda la orden ejecutada (real o simulada) y actualiza pesos de estrategias en caliente.
    """
    symbol = orden.get('symbol')
    if not symbol or 'estrategias_activas' not in orden:
        print('⚠️ Orden incompleta, no se puede registrar.')
        return
    try:
        fname = normalizar_symbol_parquet_filename(symbol)
    except ValueError as exc:
        print(f'⚠️ Símbolo inválido para persistencia: {exc}')
        return
    timestamp = orden.get('timestamp', datetime.now(UTC).timestamp())
    orden['timestamp'] = timestamp
    df_orden = pd.DataFrame([orden])
    ruta_archivo = os.path.join(CARPETA_ORDENES, fname)
    if os.path.exists(ruta_archivo):
        df_existente = pd.read_parquet(ruta_archivo)
        df_ordenes = pd.concat([df_existente, df_orden], ignore_index=True)
    else:
        df_ordenes = df_orden
    df_ordenes.to_parquet(ruta_archivo, index=False)
    print(f'📝 Orden guardada en {ruta_archivo}')
    if len(df_ordenes) >= 20:
        df_metricas = analizar_estrategias_en_ordenes(ruta_archivo)
        if not df_metricas.empty:
            valores = dict(zip(df_metricas['estrategia'], df_metricas[
                'retorno_total']))
            temp_path = str(entry_weights_temp_path())
            calculados = ajustar_pesos_por_desempeno({symbol: valores},
                temp_path)
            nuevos_pesos = calculados.get(symbol, {})
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if nuevos_pesos:
                datos = gestor_pesos.pesos
                pesos_previos = datos.get(symbol, {})
                pesos_previos.update(nuevos_pesos)
                datos[symbol] = pesos_previos
                persist_entry_weights(
                    gestor_pesos,
                    datos,
                    source=EntryWeightSource.GESTOR_APRENDIZAJE_TRADE,
                    detail=symbol,
                )
                print('✅ Pesos actualizados tras operación.')
