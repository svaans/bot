from __future__ import annotations
from datetime import datetime
import pandas as pd
from core.utils import configurar_logger
from core.adaptador_dinamico import calcular_umbral_adaptativo, calcular_tp_sl_adaptativos
from core.config_manager.dinamica import adaptar_configuracion
from core.data import coincidencia_parcial
from core.estrategias import filtrar_por_direccion
from core.strategies.tendencia import detectar_tendencia
from core.strategies.evaluador_tecnico import evaluar_puntaje_tecnico, calcular_umbral_adaptativo as calc_umbral_tecnico, cargar_pesos_tecnicos
from indicators.helpers import get_rsi, get_momentum, get_atr
from core.utils.utils import distancia_minima_valida, verificar_integridad_datos
from core.contexto_externo import obtener_puntaje_contexto
from core.metricas_semanales import metricas_tracker
log = configurar_logger('verificar_entrada')


async def verificar_entrada(trader, symbol: str, df: pd.DataFrame, estado) ->(
    dict | None):
    log.info('➡️ Entrando en verificar_entrada()')
    """
    Evalúa condiciones de entrada y devuelve info de operación
    si cumple todos los filtros, de lo contrario None.
    """
    if not verificar_integridad_datos(df):
        log.warning(f'[{symbol}] Datos de mercado incompletos o corruptos')
        metricas_tracker.registrar_filtro('datos_invalidos')
        return None
    config = adaptar_configuracion(symbol, df, trader.config_por_simbolo.get(symbol, {}))
    trader.config_por_simbolo[symbol] = config
    tendencia = trader.estado_tendencia.get(symbol)
    if not tendencia:
        tendencia, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia
    log.debug(f'[{symbol}] Tendencia: {tendencia}')
    engine_eval = trader.engine.evaluar_entrada(
        symbol,
        df,
        tendencia=tendencia,
        config={
            **config,
            "contradicciones_bloquean_entrada": getattr(
                trader, "contradicciones_bloquean_entrada", True
            ),
            "usar_score_tecnico": getattr(trader, "usar_score_tecnico", True),
        },
        pesos_symbol=trader.pesos_por_simbolo.get(symbol, {}),
    )
    estrategias = engine_eval.get('estrategias_activas', {})
    if not estrategias:
        log.warning(f'[{symbol}] Sin estrategias activas tras engine.')
        metricas_tracker.registrar_filtro('sin_estrategias')
        return None
    log.debug(f'[{symbol}] Estrategias activas: {list(estrategias.keys())}')
    estado.estrategias_buffer[-1] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)
    buffer_len = len(estado.buffer)
    persistencia_score = coincidencia_parcial(estado.estrategias_buffer[-100:], trader.pesos_por_simbolo.get(symbol, {}), ventanas=5)
    if buffer_len < 30 and persistencia_score < 1:
        metricas_tracker.registrar_filtro('prebuffer')
        return None
    umbral = calcular_umbral_adaptativo(symbol, df, estrategias, trader.
        pesos_por_simbolo.get(symbol, {}), persistencia_score)
    estrategias_persistentes = {e: (True) for e, activo in estrategias.
        items() if activo and trader.persistencia.es_persistente(symbol, e)}
    direccion = 'short' if tendencia == 'bajista' else 'long'
    estrategias_persistentes, incoherentes = filtrar_por_direccion(
        estrategias_persistentes, direccion)
    penalizacion = 0.05 * len(incoherentes) if incoherentes else 0.0
    puntaje = sum(trader.pesos_por_simbolo.get(e, 0) for e in
        estrategias_persistentes)
    puntaje += trader.persistencia.peso_extra * len(estrategias_persistentes)
    puntaje -= penalizacion
    cierre = trader.historial_cierres.get(symbol)
    if cierre:
        motivo = cierre.get('motivo')
        if motivo == 'stop loss':
            velas = cierre.get('velas', 0) + 1
            cierre['velas'] = velas
            if velas < int(config.get('cooldown_tras_perdida', 5)):
                log.info(
                    f'[{symbol}] Cooldown tras stop loss ({velas}) activo.')
                metricas_tracker.registrar_filtro('cooldown')
                return None
            trader.historial_cierres.pop(symbol, None)
        elif motivo == 'cambio de tendencia':
            precio_actual = float(df['close'].iloc[-1])
            if not trader._validar_reentrada_tendencia(symbol, df, cierre,
                precio_actual):
                cierre['velas'] = cierre.get('velas', 0) + 1
                metricas_tracker.registrar_filtro('reentrada_tendencia')
                return None
            trader.historial_cierres.pop(symbol, None)
    hoy = datetime.utcnow().date().isoformat()
    limite_base = getattr(trader.config, 'max_perdidas_diarias', 6)
    try:
        ultimo = pd.to_datetime(df['timestamp'].iloc[-1])
        inicio = ultimo - pd.Timedelta(hours=24)
        df_dia = df[pd.to_datetime(df['timestamp']) >= inicio]
        volatilidad_dia = df_dia['close'].pct_change().std()
    except Exception:
        volatilidad_dia = df['close'].pct_change().tail(1440).std()
    if volatilidad_dia > 0.05:
        limite = max(3, int(limite_base * 0.5))
    elif volatilidad_dia < 0.02:
        limite = int(limite_base * 1.2)
    else:
        limite = limite_base
    if cierre and cierre.get('fecha_perdidas') == hoy and cierre.get(
        'perdidas_consecutivas', 0) >= limite:
        log.info(f'[{symbol}] Bloqueado por pérdidas consecutivas: {limite}')
        metricas_tracker.registrar_filtro('perdidas_consecutivas')
        return None
    peso_total = sum(trader.pesos_por_simbolo.get(e, 0) for e in
        estrategias_persistentes)
    peso_min_total = config.get('peso_minimo_total', 0.5)
    diversidad_min = config.get('diversidad_minima', 2)
    rsi = engine_eval.get('rsi')
    if rsi is None:
        rsi = get_rsi(df)
    momentum = engine_eval.get('momentum')
    if momentum is None:
        momentum = get_momentum(df)
    if trader.usar_score_tecnico:
        score_tecnico, puntos_tecnicos = trader._calcular_score_tecnico(df, rsi,
            momentum, tendencia, direccion)
    else:
        score_tecnico = None
        puntos_tecnicos = None
    ok_pers, valor_pers, minimo_pers = trader._evaluar_persistencia(symbol,
        estado, df, trader.pesos_por_simbolo.get(symbol, {}), tendencia,
        puntaje, umbral, estrategias)
    razones = []
    if not trader._validar_puntaje(symbol, puntaje, umbral, config.get(
        'modo_agresivo', False)):
        razones.append('puntaje')
    diversidad_ok = await trader._validar_diversidad(symbol, peso_total,
        peso_min_total, estrategias_persistentes, diversidad_min, trader.
        pesos_por_simbolo.get(symbol, {}), df, config.get('modo_agresivo',
        False))
    if not diversidad_ok:
        umbral_peso_unico = config.get('umbral_peso_estrategia_unica',
            peso_min_total * 1.5)
        umbral_score_unico = config.get('umbral_score_estrategia_unica',
            trader.umbral_score_tecnico * 1.5)
        high_weight = peso_total >= umbral_peso_unico
        high_score = (score_tecnico or 0) >= umbral_score_unico
        if not (high_weight or high_score or config.get('modo_agresivo', False)):
            razones.append('diversidad')
    if not trader._validar_estrategia(symbol, df, estrategias, config):
        razones.append('estrategia')
    if not ok_pers:
        razones.append('persistencia')
    if razones:
        agresivo = config.get('modo_agresivo', False)
        if not agresivo or len(razones) > 2:
            log.info(f'[{symbol}] Rechazo por: {razones}')
            for r in razones:
                metricas_tracker.registrar_filtro(r)
            return None
    if trader.usar_score_tecnico:
        log.debug(
            f'[{symbol}] Score técnico {score_tecnico:.2f} componentes: {puntos_tecnicos}'
            )
    else:
        score_tecnico = None
    precio = float(df['close'].iloc[-1])
    sl, tp = calcular_tp_sl_adaptativos(symbol, df, config,
        trader.capital_por_simbolo.get(symbol, 0), precio)
    try:
        df_htf = df.set_index(pd.to_datetime(df['timestamp'])).resample('5min').last()
        if len(df_htf) >= 60:
            tendencia_htf, _ = detectar_tendencia(symbol, df_htf)
            if tendencia_htf != tendencia:
                ajuste = 0.8
                if direccion == 'long':
                    tp *= ajuste
                else:
                    sl *= ajuste
        else:
            log.warning(f'[{symbol}] ⚠️ Datos insuficientes para tendencia HTF')
            metricas_tracker.registrar_filtro('tendencia_htf_insuficiente')
    except Exception as e:
        log.error(f'❌ Error evaluando tendencia HTF para {symbol}: {e}')
        metricas_tracker.registrar_filtro('tendencia_htf_error')
    if not distancia_minima_valida(precio, sl, tp):
        log.warning(
            f'[{symbol}] SL/TP distancia mínima no válida: SL {sl} TP {tp}')
        metricas_tracker.registrar_filtro('sl_tp')
        return None
    
    # El trader validará SL y TP con ATR al abrir la orden, por lo que
    # evitamos duplicar esta comprobación aquí. Solo calculamos ATR una vez
    # para posibles ajustes posteriores.
    atr = get_atr(df)
    eval_tecnica = evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = eval_tecnica['score_total']
    score_normalizado = eval_tecnica.get('score_normalizado')
    if 'volume' in df.columns:
        ventana_vol = min(50, len(df))
        vol_media = df['volume'].rolling(ventana_vol).mean().iloc[-1]
        vol = df['volume'].iloc[-1] / (vol_media or 1)
    else:
        vol = 0
    # Las validaciones de volumen ya se realizan en el motor de estrategias y
    # afectan al score técnico, por lo que aquí solo calculamos la relación para
    # ajustar el umbral dinámico.
    volatilidad = df['close'].pct_change().tail(20).std()
    pesos_simbolo = cargar_pesos_tecnicos(symbol)
    score_max = sum(pesos_simbolo.values())
    if score_normalizado is None:
        score_normalizado = score_total / score_max if score_max else score_total
    umbral_tecnico = calc_umbral_tecnico(score_max, tendencia, volatilidad,
        vol, estrategias_persistentes)
    umbral_normalizado = umbral_tecnico / score_max if score_max else umbral_tecnico
    if score_normalizado < umbral_normalizado:
        log.info(
            f'[{symbol}] Score técnico {score_normalizado:.2f} < umbral {umbral_normalizado:.2f}'
            )
        metricas_tracker.registrar_filtro('score_tecnico')
        return None
    abiertas = [s for s, o in trader.orders.ordenes.items() if o.cantidad_abierta > 0 and s != symbol]
    if abiertas:
        correlaciones = trader._calcular_correlaciones(symbols=[symbol, *abiertas])
        if not correlaciones.empty and symbol in correlaciones.columns:
            corr = correlaciones.loc[symbol, abiertas].abs().max()
            umbral_corr = getattr(trader.config, 'umbral_correlacion', 0.9)
            if corr >= umbral_corr:
                log.info(f'[{symbol}] Correlación {corr:.2f} supera umbral {umbral_corr}')
                metricas_tracker.registrar_filtro('correlacion')
                return None
    puntaje_macro = obtener_puntaje_contexto(symbol)
    if abs(puntaje_macro) > getattr(trader.config, 'umbral_puntaje_macro', 6):
        log.info(f'[{symbol}] Contexto macro desfavorable ({puntaje_macro:.2f})')
        metricas_tracker.registrar_filtro('contexto_macro')
        return None
    log.info(
        f'✅ [{symbol}] Señal de entrada generada con {len(estrategias_persistentes)} estrategias activas.'
        )
    return {'symbol': symbol, 'precio': precio, 'sl': sl, 'tp': tp,
        'estrategias': estrategias_persistentes, 'puntaje': puntaje,
        'umbral': umbral, 'tendencia': tendencia, 'direccion': direccion,
        'score_tecnico': score_tecnico, 'detalles_tecnicos': eval_tecnica.
        get('detalles', {})}
