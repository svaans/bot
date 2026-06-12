import os
import glob
from dotenv import dotenv_values
from learning.analisis_resultados import analizar_estrategias_en_ordenes
from core.strategies.ajustador_pesos import ajustar_pesos_por_desempeno
from core.strategies.pesos import (
    _normalize_with_floor,
    entry_weights_temp_path,
    gestor_pesos,
)
from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights
from learning import historial_operaciones as _hist
CONFIG = dotenv_values('config/claves.env')
MODO_REAL = CONFIG.get('MODO_REAL', 'False') == 'True'
def recalibrar_pesos_semana() ->None:
    # Misma fuente que el writer: ruta absoluta basada en la raíz del repo.
    carpeta = str(_hist.CARPETA_ORDENES)
    archivos = glob.glob(os.path.join(carpeta, '*.parquet'))
    if not archivos:
        print(f'⚠️ No se encontraron archivos de órdenes en {carpeta}')
        return
    resultados = {}
    for ruta in archivos:
        symbol = os.path.splitext(os.path.basename(ruta))[0].replace('_', '/')
        df_metricas = analizar_estrategias_en_ordenes(ruta)
        if df_metricas.empty:
            continue
        valores = dict(zip(df_metricas['estrategia'], df_metricas[
            'retorno_total']))
        resultados[symbol] = valores
    if not resultados:
        print('⚠️ Sin métricas válidas para recalibrar.')
        return
    temp_path = str(entry_weights_temp_path())
    pesos_crudos = ajustar_pesos_por_desempeno(resultados, temp_path)
    # Normalizar cada símbolo a la escala del gestor (total=100, piso=1):
    # normalizar_scores (÷max) produciría pesos en 0-1, hundiendo el
    # puntaje_total por debajo del umbral de entrada. Además, guardar(snapshot)
    # REEMPLAZA el mapa completo: se hace merge para no borrar los pesos de
    # símbolos sin órdenes esta semana.
    pesos_normalizados = {
        symbol: _normalize_with_floor(
            data, total=gestor_pesos.total, piso=gestor_pesos.piso
        )
        for symbol, data in pesos_crudos.items()
    }
    snapshot = {**gestor_pesos.pesos, **pesos_normalizados}
    persist_entry_weights(
        gestor_pesos,
        snapshot,
        source=EntryWeightSource.RECALIBRAR_SEMANA,
        detail="all_symbols_parquet",
    )
    if os.path.exists(temp_path):
        os.remove(temp_path)
    print('✅ Pesos recalibrados correctamente.')


if __name__ == '__main__':
    recalibrar_pesos_semana()
