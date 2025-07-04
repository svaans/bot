from __future__ import annotations
from datetime import datetime
import pandas as pd
from core.utils import configurar_logger
from core.adaptador_dinamico import calcular_umbral_adaptativo, calcular_tp_sl_adaptativos
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.data import coincidencia_parcial
from core.estrategias import filtrar_por_direccion
from core.strategies.tendencia import detectar_tendencia
from core.strategies.evaluador_tecnico import evaluar_puntaje_tecnico, calcular_umbral_adaptativo as calc_umbral_tecnico, cargar_pesos_tecnicos
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope
from core.utils.utils import distancia_minima_valida
log = configurar_logger('verificar_entrada')


async def verificar_entrada(trader, symbol: str, df: pd.DataFrame, estado) ->(
    dict | None):
    """Evalúa las condiciones de entrada y devuelve info de la operación."""
    config_actual = trader.config_por_simbolo.get(symbol, {})
    dinamica = adaptar_configuracion(symbol, df)
    if dinamica:
        config_actual.update(dinamica)
    config_actual = adaptar_configuracion_base(symbol, df, config_actual)
    trader.config_por_simbolo[symbol] = config_actual
    tendencia_actual = trader.estado_tendencia.get(symbol)
    if not tendencia_actual:
        tendencia_actual, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia_actual
    log.debug(f'[{symbol}] Tendencia detectada: {tendencia_actual}')
    evaluacion = trader.engine.evaluar_entrada(symbol, df, tendencia=
        tendencia_actual, config=config_actual, pesos_symbol=trader.
        pesos_por_simbolo.get(symbol, {}))
    estrategias = evaluacion.get('estrategias_activas', {})
    log.debug(f'[{symbol}] Estrategias iniciales desde engine: {estrategias}')
    if not estrategias:
        log.warning(
            f'⚠️ [{symbol}] Sin estrategias activas tras evaluación. Tendencia detectada previamente.'
            )
    else:
        log.info(
            f'🧪 [{symbol}] Estrategias activas: {list(estrategias.keys())}')
    estado.buffer[-1]['estrategias_activas'] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)
    pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
    if len(estado.buffer) < 30:
        persistencia = coincidencia_parcial(estado.buffer, pesos_symbol,
            ventanas=5)
        log.debug(
            f'[{symbol}] Persistencia parcial (buffer corto): {persistencia:.2f}'
            )
        if persistencia < 1:
            return None
    persistencia_score = coincidencia_parcial(estado.buffer, pesos_symbol,
        ventanas=5)
    umbral = calcular_umbral_adaptativo(symbol, df, estrategias,
        pesos_symbol, persistencia=persistencia_score)
    estrategias_persistentes = {e: (True) for e, act in estrategias.items() if
        act and trader.persistencia.es_persistente(symbol, e)}
    log.debug(
        f'[{symbol}] Estrategias persistentes: {estrategias_persistentes}')
    if not estrategias_persistentes:
        log.warning(
            f'[{symbol}] Ninguna estrategia pasó el filtro de persistencia.')
        return None
    direccion = 'short' if tendencia_actual == 'bajista' else 'long'
    estrategias_persistentes, incoherentes = filtrar_por_direccion(
        estrategias_persistentes, direccion)
    log.debug(
        f'[{symbol}] Después del filtro por dirección ({direccion}): {estrategias_persistentes}'
        )
    log.debug(f'[{symbol}] Estrategias incoherentes: {incoherentes}')
    if not estrategias_persistentes:
        log.warning(
            f'[{symbol}] Estrategias incoherentes con la dirección {direccion}.'
            )
        return None
    penalizacion = 0.05 * len(incoherentes) ** 2 if incoherentes else 0.0
    puntaje = sum(pesos_symbol.get(k, 0) for k in estrategias_persistentes)
    puntaje += trader.persistencia.peso_extra * len(estrategias_persistentes)
    puntaje -= penalizacion
    estado.ultimo_umbral = umbral
    cierre = trader.historial_cierres.get(symbol)
    if cierre:
        motivo = cierre.get('motivo')
        if motivo == 'stop loss':
            cooldown_velas = int(config_actual.get('cooldown_tras_perdida', 5))
            velas = cierre.get('velas', 0)
            if velas < cooldown_velas:
                cierre['velas'] = velas + 1
                restante = cooldown_velas - velas
                log.info(
                    f'🕒 [{symbol}] Cooldown activo por stop loss. Quedan {restante} velas.'
                    )
                return None
            else:
                trader.historial_cierres.pop(symbol, None)
        elif motivo == 'cambio de tendencia':
            precio_actual = float(df['close'].iloc[-1])
            if not trader._validar_reentrada_tendencia(symbol, df, cierre,
                precio_actual):
                cierre['velas'] = cierre.get('velas', 0) + 1
                log.info(
                    f'🚫 [{symbol}] Reentrada bloqueada por cambio de tendencia.'
                    )
                return None
            else:
                trader.historial_cierres.pop(symbol, None)
    registro = cierre or {}
    fecha_hoy = datetime.utcnow().date().isoformat()
    if registro.get('fecha_perdidas') == fecha_hoy and registro.get(
        'perdidas_consecutivas', 0) >= 6:
        log.info(f'🚫 [{symbol}] Bloqueo por pérdidas consecutivas en el día.')
        return None
    estrategias_activas = {e: pesos_symbol.get(e, 0.0) for e in
        estrategias_persistentes}
    peso_total = sum(estrategias_activas.values())
    peso_min_total = config_actual.get('peso_minimo_total', 0.5)
    diversidad_min = config_actual.get('diversidad_minima', 2)
    persistencia = coincidencia_parcial(estado.buffer, pesos_symbol, ventanas=5
        )
    log.info(
        f'📊 [{symbol}] Puntaje: {puntaje:.2f}, Umbral: {umbral:.2f}, Peso total: {peso_total:.2f}, Persistencia: {persistencia:.2f}, Estrategias activas: {estrategias_activas}'
        )
    razones: list[str] = []
    if not trader._validar_puntaje(symbol, puntaje, umbral, config_actual.
        get('modo_agresivo', False)):
        razones.append('puntaje')
    if not await trader._validar_diversidad(symbol, peso_total,
        peso_min_total, estrategias_activas, diversidad_min, pesos_symbol,
        df, config_actual.get('modo_agresivo', False)):
        razones.append('diversidad')
    if not trader._validar_estrategia(symbol, df, estrategias):
        razones.append('estrategia')
    ok_pers, valor_pers, minimo_pers = trader._evaluar_persistencia(symbol,
        estado, df, pesos_symbol, tendencia_actual, puntaje, umbral,
        estrategias)
    if not ok_pers:
        razones.append('persistencia')
    if razones:
        agresivo = config_actual.get('modo_agresivo', False)
        if not agresivo or len(razones) > 2:
            log.info(f'❌ [{symbol}] Rechazo acumulado por: {razones}')
            return None
    rsi = calcular_rsi(df)
    momentum = calcular_momentum(df)
    slope = calcular_slope(df)
    precio_actual = float(df['close'].iloc[-1])
    cantidad_simulada = await trader._calcular_cantidad_async(symbol,
        precio_actual)
    if trader.usar_score_tecnico:
        score_tecnico, puntos = trader._calcular_score_tecnico(df, rsi,
            momentum, tendencia_actual, direccion)
        log.debug(
            f'[{symbol}] Score técnico: {score_tecnico:.2f}, Componentes: {puntos}'
            )
    if puntaje < umbral or not estrategias_persistentes:
        log.info(f'❌ [{symbol}] Filtro técnico final bloqueó la entrada.')
        return None
    log.info(
        f'✅ [{symbol}] Señal de entrada generada con {len(estrategias_activas)} estrategias.'
        )
    precio = precio_actual
    sl, tp = calcular_tp_sl_adaptativos(symbol, df, config_actual, trader.
        capital_por_simbolo.get(symbol, 0), precio)
    if not distancia_minima_valida(precio, sl, tp):
        log.warning(
            f'📏 [{symbol}] Distancia SL/TP insuficiente. SL: {sl:.2f} TP: {tp:.2f}'
            )
        return None
    evaluacion = evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = evaluacion['score_total']
    vol = 0.0
    if 'volume' in df.columns and len(df) > 20:
        vol = df['volume'].iloc[-1] / (df['volume'].rolling(20).mean().iloc
            [-1] or 1)
    volatilidad = df['close'].pct_change().tail(20).std()
    pesos_simbolo = cargar_pesos_tecnicos(symbol)
    score_max = sum(pesos_simbolo.values())
    umbral_tecnico = calc_umbral_tecnico(score_max, tendencia_actual,
        volatilidad, vol, estrategias_persistentes)
    log.info(
        f"- Umbral adaptativo: {umbral_tecnico:.2f} → {'✅' if score_total >= umbral_tecnico else '❌'} Entrada permitida"
        )
    if score_total < umbral_tecnico:
        log.info(
            f'[{symbol}] Entrada rechazada por score técnico {score_total:.2f} < {umbral_tecnico:.2f}'
            )
        return None
    return {'symbol': symbol, 'precio': precio, 'sl': sl, 'tp': tp,
        'estrategias': estrategias_activas, 'puntaje': puntaje, 'umbral':
        umbral, 'tendencia': tendencia_actual, 'direccion': direccion,
        'score_tecnico': score_tecnico if trader.usar_score_tecnico else
        None, 'detalles_tecnicos': evaluacion.get('detalles', {})}
