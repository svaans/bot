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


async def verificar_entrada(trader, symbol: str, df: pd.DataFrame, estado) -> dict | None:
    """
    Evalúa condiciones de entrada y devuelve info de operación
    si cumple todos los filtros, de lo contrario None.
    """
    # variables iniciales
    config = trader.config_por_simbolo.get(symbol, {})
    config.update(adaptar_configuracion(symbol, df) or {})
    config = adaptar_configuracion_base(symbol, df, config)
    trader.config_por_simbolo[symbol] = config

    # tendencia actual
    tendencia = trader.estado_tendencia.get(symbol)
    if not tendencia:
        tendencia, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia
    log.debug(f"[{symbol}] Tendencia: {tendencia}")

    # evaluar engine
    engine_eval = trader.engine.evaluar_entrada(
        symbol,
        df,
        tendencia=tendencia,
        config=config,
        pesos_symbol=trader.pesos_por_simbolo.get(symbol, {})
    )
    estrategias = engine_eval.get('estrategias_activas', {})
    if not estrategias:
        log.warning(f"[{symbol}] Sin estrategias activas tras engine.")
        return None

    log.debug(f"[{symbol}] Estrategias activas: {list(estrategias.keys())}")
    estado.buffer[-1]['estrategias_activas'] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)

    # buffer corto → persistencia
    buffer_len = len(estado.buffer)
    persistencia_score = coincidencia_parcial(estado.buffer[-100:], trader.pesos_por_simbolo.get(symbol, {}), ventanas=5)
    if buffer_len < 30 and persistencia_score < 1:
        return None

    # umbral
    umbral = calcular_umbral_adaptativo(symbol, df, estrategias, trader.pesos_por_simbolo.get(symbol, {}), persistencia_score)
    estrategias_persistentes = {
        e: True
        for e, activo in estrategias.items()
        if activo and trader.persistencia.es_persistente(symbol, e)
    }

    # filtrar por dirección
    direccion = "short" if tendencia == "bajista" else "long"
    estrategias_persistentes, incoherentes = filtrar_por_direccion(estrategias_persistentes, direccion)
    penalizacion = 0.05 * len(incoherentes) ** 2 if incoherentes else 0.0
    puntaje = sum(trader.pesos_por_simbolo.get(e, 0) for e in estrategias_persistentes)
    puntaje += trader.persistencia.peso_extra * len(estrategias_persistentes)
    puntaje -= penalizacion

    # cool down
    cierre = trader.historial_cierres.get(symbol)
    if cierre:
        motivo = cierre.get('motivo')
        if motivo == 'stop loss':
            velas = cierre.get('velas', 0) + 1
            cierre['velas'] = velas
            if velas < int(config.get('cooldown_tras_perdida', 5)):
                log.info(f"[{symbol}] Cooldown tras stop loss ({velas}) activo.")
                return None
            trader.historial_cierres.pop(symbol, None)
        elif motivo == 'cambio de tendencia':
            precio_actual = float(df['close'].iloc[-1])
            if not trader._validar_reentrada_tendencia(symbol, df, cierre, precio_actual):
                cierre['velas'] = cierre.get('velas', 0) + 1
                return None
            trader.historial_cierres.pop(symbol, None)

    # bloqueos de pérdidas consecutivas
    hoy = datetime.utcnow().date().isoformat()
    if cierre and cierre.get('fecha_perdidas') == hoy and cierre.get('perdidas_consecutivas', 0) >= 6:
        log.info(f"[{symbol}] Bloqueado por pérdidas consecutivas en el día.")
        return None

    # validaciones de consistencia
    peso_total = sum(trader.pesos_por_simbolo.get(e, 0) for e in estrategias_persistentes)
    peso_min_total = config.get('peso_minimo_total', 0.5)
    diversidad_min = config.get('diversidad_minima', 2)
    ok_pers, valor_pers, minimo_pers = trader._evaluar_persistencia(
        symbol, estado, df, trader.pesos_por_simbolo.get(symbol, {}), tendencia, puntaje, umbral, estrategias
    )

    razones = []
    if not trader._validar_puntaje(symbol, puntaje, umbral, config.get('modo_agresivo', False)):
        razones.append('puntaje')
    if not await trader._validar_diversidad(
        symbol, peso_total, peso_min_total, estrategias_persistentes, diversidad_min,
        trader.pesos_por_simbolo.get(symbol, {}), df, config.get('modo_agresivo', False)
    ):
        razones.append('diversidad')
    if not trader._validar_estrategia(symbol, df, estrategias):
        razones.append('estrategia')
    if not ok_pers:
        razones.append('persistencia')

    if razones:
        agresivo = config.get('modo_agresivo', False)
        if not agresivo or len(razones) > 2:
            log.info(f"[{symbol}] Rechazo por: {razones}")
            return None

    # score técnico
    if trader.usar_score_tecnico:
        rsi = calcular_rsi(df)
        momentum = calcular_momentum(df)
        slope = calcular_slope(df)
        score_tecnico, puntos = trader._calcular_score_tecnico(df, rsi, momentum, tendencia, direccion)
        log.debug(f"[{symbol}] Score técnico {score_tecnico:.2f} componentes: {puntos}")
    else:
        score_tecnico = None

    # SL y TP
    precio = float(df['close'].iloc[-1])
    sl, tp = calcular_tp_sl_adaptativos(
        symbol, df, config, trader.capital_por_simbolo.get(symbol, 0), precio
    )
    if not distancia_minima_valida(precio, sl, tp):
        log.warning(f"[{symbol}] SL/TP distancia mínima no válida: SL {sl} TP {tp}")
        return None

    # score técnico final
    eval_tecnica = evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = eval_tecnica['score_total']
    vol = df['volume'].iloc[-1] / (df['volume'].rolling(20).mean().iloc[-1] or 1) if 'volume' in df.columns else 0
    volatilidad = df['close'].pct_change().tail(20).std()
    pesos_simbolo = cargar_pesos_tecnicos(symbol)
    score_max = sum(pesos_simbolo.values())
    umbral_tecnico = calc_umbral_tecnico(score_max, tendencia, volatilidad, vol, estrategias_persistentes)

    if score_total < umbral_tecnico:
        log.info(f"[{symbol}] Score técnico {score_total:.2f} menor que umbral {umbral_tecnico:.2f}")
        return None

    # resultado
    log.info(f"✅ [{symbol}] Señal de entrada generada con {len(estrategias_persistentes)} estrategias activas.")
    return {
        "symbol": symbol,
        "precio": precio,
        "sl": sl,
        "tp": tp,
        "estrategias": estrategias_persistentes,
        "puntaje": puntaje,
        "umbral": umbral,
        "tendencia": tendencia,
        "direccion": direccion,
        "score_tecnico": score_tecnico,
        "detalles_tecnicos": eval_tecnica.get("detalles", {})
    }

