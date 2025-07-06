import pandas as pd
import inspect
from core.utils.utils import validar_dataframe
from core.strategies.tendencia import detectar_tendencia
from core.strategies.entry.gestor_entradas import evaluar_estrategias
from core.adaptador_dinamico import calcular_umbral_adaptativo
from core.adaptador_umbral import calcular_umbral_salida_adaptativo
from core.strategies.pesos import obtener_peso_salida
from core.utils import configurar_logger
log = configurar_logger('gestor_salidas')
from .loader_salidas import cargar_estrategias_salida


def evaluar_salidas(orden: dict, df, config=None, contexto=None):
    log.info('➡️ Entrando en evaluar_salidas()')
    symbol = orden.get('symbol', 'SYM')
    if not validar_dataframe(df, ['close', 'high', 'low', 'volume']):
        log.warning(f'[{symbol}] DataFrame inválido para gestor de salidas')
        return {'cerrar': False, 'razon': 'Datos insuficientes'}
    funciones = cargar_estrategias_salida()
    resultados = []
    PRIORIDAD_ABSOLUTA = {'Stop Loss', 'Estrategia: Cambio de tendencia'}
    for f in funciones:
        if not callable(f):
            continue
        try:
            params = list(inspect.signature(f).parameters.keys())
            if 'symbol' in params and 'orden' in params and 'config' in params:
                resultado = f(symbol, orden, df, config=config)
            elif 'symbol' in params and 'orden' in params:
                resultado = f(symbol, orden, df)
            elif 'symbol' in params and 'config' in params:
                resultado = f(symbol, df, config=config)
            elif 'symbol' in params:
                resultado = f(symbol, df)
            elif 'orden' in params and 'config' in params:
                resultado = f(orden, df, config=config)
            elif 'orden' in params:
                resultado = f(orden, df)
            elif 'config' in params:
                resultado = f(df, config=config)
            else:
                resultado = f(df)
        except Exception as e:
            log.warning(f'❌ Error ejecutando estrategia de salida: {f} → {e}')
            continue
        if resultado.get('cerrar', False):
            evento = resultado.get('evento', resultado.get('razon',
                'Sin motivo'))
            razon = resultado.get('razon', 'Sin motivo')
            if evento in PRIORIDAD_ABSOLUTA:
                log.info(f'[{symbol}] Cierre prioritario por {evento}')
                return {'cerrar': True, 'razon': evento}
            resultados.append(evento)
    peso_total = sum(obtener_peso_salida(razon, symbol) for razon in resultados
        )
    umbral = calcular_umbral_salida_adaptativo(symbol, config or {}, contexto)
    min_conf = (config or {}).get('min_confirmaciones_salida', 1)
    log.info(
        f'[SALIDA] {symbol} | Score: {peso_total:.2f} | Umbral: {umbral:.2f} | Señales: {resultados}'
        )
    cerrar = peso_total >= umbral and len(resultados) >= min_conf
    if cerrar:
        razon = (
            f'Score {peso_total:.2f} ≥ {umbral:.2f} con {len(resultados)} señales'
            )
    else:
        razon = (f'Score insuficiente {peso_total:.2f} < {umbral:.2f}' if 
            len(resultados) >= min_conf else
            f'Señales insuficientes: {len(resultados)}/{min_conf}')
    return {'cerrar': cerrar, 'razon': razon, 'detalles': resultados,
        'score': peso_total, 'umbral': umbral}


def verificar_filtro_tecnico(symbol, df, estrategias_activas, pesos_symbol,
    config=None):
    log.info('➡️ Entrando en verificar_filtro_tecnico()')
    if not validar_dataframe(df, ['high', 'low', 'close']):
        return False
    tendencia, _ = detectar_tendencia(symbol, df)
    evaluacion = evaluar_estrategias(symbol, df, tendencia)
    if not evaluacion:
        return False
    activas = [k for k, v in evaluacion['estrategias_activas'].items() if v]
    puntaje = evaluacion['puntaje_total']
    umbral = calcular_umbral_adaptativo(symbol, df, evaluacion[
        'estrategias_activas'], pesos_symbol, persistencia=0.0, config=config)
    return len(activas) >= 1 and puntaje >= 0.4 * umbral
