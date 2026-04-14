import os
import glob
import json
from datetime import datetime, timezone

UTC = timezone.utc
import pandas as pd
from dotenv import dotenv_values
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger
from learning.analisis_resultados import analizar_estrategias_en_ordenes
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from learning.recalibrar_semana import recalibrar_pesos_semana
from core.strategies.pesos import gestor_pesos
from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights
from config.configuracion import cargar_configuracion_simbolo, guardar_configuracion_simbolo
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.adaptador_dinamico import calcular_tp_sl_adaptativos
from core.risk import RiskManager

from .historial_operaciones import symbol_desde_parquet_stem
CONFIG = dotenv_values('config/claves.env')
MODO_REAL = CONFIG.get('MODO_REAL', 'False') == 'True'
CARPETA_ORDENES = 'ordenes_reales' if MODO_REAL else 'ordenes_simuladas'
FEEDBACK_PATH = 'config/feedback_manual.json'
log = configurar_logger('aprendizaje_continuo')


def _cargar_feedback(symbol: str) ->dict:
    """Lee ajustes manuales de ``FEEDBACK_PATH`` si existen."""
    if not os.path.exists(FEEDBACK_PATH):
        return {}
    try:
        with open(FEEDBACK_PATH, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        return data.get(symbol, {})
    except Exception as e:
        log.warning(
            '⚠️ Error leyendo feedback manual: %s',
            format_exception_for_log(e),
        )
        return {}


def _aplicar_feedback_pesos(symbol: str, feedback: dict) ->None:
    if not feedback.get('pesos'):
        return
    pesos = gestor_pesos.obtener_pesos_symbol(symbol)
    if not pesos:
        return
    actualizado = False
    for estrategia, ajuste in feedback['pesos'].items():
        if estrategia in pesos:
            try:
                pesos[estrategia] = max(0.0, pesos[estrategia] + float(ajuste))
                actualizado = True
            except Exception:
                continue
    if actualizado:
        datos = gestor_pesos.pesos
        datos[symbol] = pesos
        persist_entry_weights(
            gestor_pesos,
            datos,
            source=EntryWeightSource.FEEDBACK_MANUAL,
            detail=symbol,
        )
        log.info(f'📝 Feedback aplicado a pesos de {symbol}')


def _actualizar_config(symbol: str, df: pd.DataFrame) ->None:
    if df.empty:
        return
    config = cargar_configuracion_simbolo(symbol) or {}
    umbral = calcular_umbral_adaptativo(symbol, df)
    config['umbral_adaptativo'] = round(float(umbral), 2)
    precio_actual = float(df['close'].iloc[-1]
        ) if 'close' in df.columns else None
    if precio_actual is not None and {'high', 'low', 'close'}.issubset(df.
        columns):
        sl, tp = calcular_tp_sl_adaptativos(symbol, df, config, None,
            precio_actual)
        tmp = df.copy()
        tmp['hl'] = tmp['high'] - tmp['low']
        tmp['hc'] = abs(tmp['high'] - tmp['close'].shift(1))
        tmp['lc'] = abs(tmp['low'] - tmp['close'].shift(1))
        tmp['tr'] = tmp[['hl', 'hc', 'lc']].max(axis=1)
        atr = tmp['tr'].rolling(window=14).mean().iloc[-1]
        if pd.isna(atr):
            atr = precio_actual * 0.01
        config['sl_ratio'] = round(abs(precio_actual - sl) / atr, 2)
        config['tp_ratio'] = round(abs(tp - precio_actual) / atr, 2)
    guardar_configuracion_simbolo(symbol, config)
    log.info(f'🔧 Configuración actualizada para {symbol}')


def procesar_simbolo(symbol: str, ruta: str) ->None:
    metricas_diarias = analizar_estrategias_en_ordenes(ruta, dias=1)
    if metricas_diarias.empty:
        log.info(f'[{symbol}] Sin operaciones diarias para evaluar')
    else:
        actualizar_pesos_estrategias_symbol(symbol)
    df = pd.read_parquet(ruta)
    _actualizar_config(symbol, df.tail(60))
    feedback = _cargar_feedback(symbol)
    _aplicar_feedback_pesos(symbol, feedback)
    cfg_riesgo = cargar_configuracion_simbolo(symbol) or {}
    umbral_seed = float(cfg_riesgo.get('riesgo_maximo_diario', 0.03))
    rm = RiskManager(umbral_seed)
    semana = df[df['timestamp'] >= (datetime.now(UTC) - pd.Timedelta(days=7
        )).timestamp()]
    metricas = {'ganancia_semana': semana.get('retorno_total', pd.Series())
        .sum(), 'winrate': (semana.get('retorno_total', pd.Series()) > 0).
        mean(), 'drawdown': semana.get('retorno_total', pd.Series()).min()}
    umbral_antes = rm.umbral
    rm.ajustar_umbral(metricas)
    if rm.umbral != umbral_antes:
        cfg_riesgo = cargar_configuracion_simbolo(symbol) or {}
        cfg_riesgo['riesgo_maximo_diario'] = float(rm.umbral)
        guardar_configuracion_simbolo(symbol, cfg_riesgo)
        log.info(
            '🔧 riesgo_maximo_diario persistido para %s: %.4f → %.4f',
            symbol,
            umbral_antes,
            rm.umbral,
        )


def ejecutar_ciclo() ->None:
    archivos = glob.glob(os.path.join(CARPETA_ORDENES, '*.parquet'))
    if not archivos:
        log.warning(f'⚠️ No se encontraron órdenes en {CARPETA_ORDENES}')
        return
    for ruta in archivos:
        symbol = symbol_desde_parquet_stem(os.path.splitext(os.path.basename(ruta))[0])
        procesar_simbolo(symbol, ruta)
    if datetime.now(UTC).weekday() == 0:
        recalibrar_pesos_semana()


if __name__ == '__main__':
    ejecutar_ciclo()
