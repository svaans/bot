import os
import json
import pandas as pd
from collections import defaultdict
from core.strategies.pesos import gestor_pesos
from core.adaptador_dinamico import calcular_umbral_adaptativo, calcular_tp_sl_adaptativos
from config.configuracion import cargar_configuracion_simbolo, guardar_configuracion_simbolo
from core.utils.utils import configurar_logger
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA_OPERACIONES = os.path.join(BASE_DIR, 'ultimas_operaciones')
RUTA_PESOS = 'config/estrategias_pesos.json'
MAX_OPERACIONES = 30
MIN_OPERACIONES = 5
VENTANA_ACTUALIZACION = 10
log = configurar_logger('trader_simulado', modo_silencioso=True)
os.makedirs(CARPETA_OPERACIONES, exist_ok=True)


def registrar_resultado_trade(symbol: str, orden: dict, ganancia: float):
    log.info('➡️ Entrando en registrar_resultado_trade()')
    archivo = os.path.join(CARPETA_OPERACIONES, symbol.replace('/', '_').
        upper() + '.parquet')
    historial = []
    if os.path.exists(archivo):
        try:
            df_prev = pd.read_parquet(archivo)
            historial = df_prev.to_dict('records')
        except Exception as e:
            print(
                f'⚠️ Archivo dañado: {archivo} — se sobrescribirá. Error: {e}')
            historial = []
    estrategias_activas = orden.get('estrategias_activas', {})
    if isinstance(estrategias_activas, str):
        try:
            estrategias_activas = json.loads(estrategias_activas.replace(
                "'", '"'))
        except Exception as e:
            print(f'❌ Error al parsear estrategias activas de {symbol}: {e}')
            estrategias_activas = {}
    nueva_operacion = {'retorno_total': ganancia, 'estrategias_activas':
        estrategias_activas}
    historial.append(nueva_operacion)
    historial = historial[-MAX_OPERACIONES:]
    try:
        df_guardar = pd.DataFrame(historial)
        df_guardar.to_parquet(archivo, index=False)
    except Exception as e:
        print(f'❌ Error al guardar historial para {symbol}: {e}')
        return
    if len(historial) >= VENTANA_ACTUALIZACION and len(historial
        ) % VENTANA_ACTUALIZACION == 0:
        ventana = historial[-VENTANA_ACTUALIZACION:]
        actualizar_pesos_dinamicos(symbol, ventana)


def actualizar_pesos_dinamicos(symbol: str, historial: list, factor_ajuste=0.05
    ):
    log.info('➡️ Entrando en actualizar_pesos_dinamicos()')
    datos = defaultdict(list)
    pesos_actuales = gestor_pesos.obtener_pesos_symbol(symbol)
    for orden in historial:
        estrategias = orden.get('estrategias_activas', {})
        if isinstance(estrategias, str):
            try:
                estrategias = json.loads(estrategias.replace("'", '"'))
            except:
                continue
        retorno = orden.get('retorno_total', 0.0)
        for estrategia, activa in estrategias.items():
            if activa:
                datos[estrategia].append(retorno)
    nuevos_pesos = pesos_actuales.copy()
    for estrategia, retornos in datos.items():
        if len(retornos) < MIN_OPERACIONES:
            continue
        promedio = sum(retornos) / len(retornos)
        winrate = sum(1 for r in retornos if r > 0) / len(retornos)
        peso_anterior = nuevos_pesos.get(estrategia, 0.5)
        peso_objetivo = min(1.0, max(0.0, promedio * winrate))
        nuevos_pesos[estrategia] = peso_anterior * 0.98 + peso_objetivo * 0.02
    pesos_totales = gestor_pesos.pesos
    pesos_totales[symbol] = nuevos_pesos
    gestor_pesos.guardar(pesos_totales)
    print(f'\n🧠 Pesos ajustados dinámicamente para {symbol}:')
    for estrategia, peso in nuevos_pesos.items():
        print(f'  - {estrategia}: {peso:.3f}')
    try:
        df_fake = pd.DataFrame(historial)
        estrategias = df_fake.iloc[-1].get('estrategias_activas', {})
        if isinstance(estrategias, str):
            estrategias = json.loads(estrategias.replace("'", '"'))
        if estrategias:
            umbral = calcular_umbral_adaptativo(symbol, df_fake,
                estrategias, nuevos_pesos, persistencia=0.0)
            print(f'📈 Umbral estimado para {symbol}: {umbral:.2f}')
            config_actual = cargar_configuracion_simbolo(symbol) or {}
            config_actual['umbral_adaptativo'] = round(float(umbral), 2)
            precio_actual = None
            if 'close' in df_fake.columns:
                precio_actual = float(df_fake['close'].iloc[-1])
            if precio_actual is not None and all(c in df_fake.columns for c in
                ['high', 'low', 'close']):
                sl, tp = calcular_tp_sl_adaptativos(symbol, df_fake,
                    config_actual, None, precio_actual)
                df_tmp = df_fake.copy()
                df_tmp['hl'] = df_tmp['high'] - df_tmp['low']
                df_tmp['hc'] = abs(df_tmp['high'] - df_tmp['close'].shift(1))
                df_tmp['lc'] = abs(df_tmp['low'] - df_tmp['close'].shift(1))
                df_tmp['tr'] = df_tmp[['hl', 'hc', 'lc']].max(axis=1)
                atr = df_tmp['tr'].rolling(window=14).mean().iloc[-1]
                if pd.isna(atr):
                    atr = precio_actual * 0.01
                config_actual['sl_ratio'] = round(abs(precio_actual - sl) /
                    atr, 2)
                config_actual['tp_ratio'] = round(abs(tp - precio_actual) /
                    atr, 2)
            guardar_configuracion_simbolo(symbol, config_actual)
    except Exception as e:
        print(f'❌ Error al recalcular/guardar umbral para {symbol}: {e}')
