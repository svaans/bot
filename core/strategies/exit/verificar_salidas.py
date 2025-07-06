from __future__ import annotations
from datetime import datetime
import pandas as pd
from core.utils import configurar_logger
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.atr import calcular_atr
from core.strategies.tendencia import detectar_tendencia
from .salida_stoploss import verificar_salida_stoploss
from .salida_trailing_stop import verificar_trailing_stop
from .salida_por_tendencia import verificar_reversion_tendencia
from .gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from .analisis_previo_salida import permitir_cierre_tecnico, evaluar_condiciones_de_cierre_anticipado
from .analisis_salidas import patron_tecnico_fuerte
from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.metricas_semanales import metricas_tracker
log = configurar_logger('verificar_salidas')


async def verificar_salidas(trader, symbol: str, df: pd.DataFrame) ->None:
    log.info('➡️ Entrando en verificar_salidas()')
    """Evalúa si la orden abierta debe cerrarse."""
    orden = trader.orders.obtener(symbol)
    if not orden:
        log.warning(
            f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
        return
    orden.duracion_en_velas = getattr(orden, 'duracion_en_velas', 0) + 1
    await trader._piramidar(symbol, orden, df)
    precio_min = float(df['low'].iloc[-1])
    precio_max = float(df['high'].iloc[-1])
    precio_cierre = float(df['close'].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, {})
    log.debug(f'Verificando salidas para {symbol} con orden: {orden.to_dict()}'
        )
    atr = calcular_atr(df)
    volatilidad_rel = atr / precio_cierre if atr and precio_cierre else 1.0
    tendencia_detectada = trader.estado_tendencia.get(symbol)
    if not tendencia_detectada:
        tendencia_detectada, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia_detectada
    contexto = {'volatilidad': volatilidad_rel, 'tendencia':
        tendencia_detectada}
    if precio_min <= orden.stop_loss:
        rsi = calcular_rsi(df)
        momentum = calcular_momentum(df)
        tendencia_actual = trader.estado_tendencia.get(symbol)
        if not tendencia_actual:
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            trader.estado_tendencia[symbol] = tendencia_actual
        score, _ = trader._calcular_score_tecnico(df, rsi, momentum,
            tendencia_actual, orden.direccion)
        if score >= 2 or patron_tecnico_fuerte(df):
            log.info(
                f'🛡️ SL evitado por validación técnica — Score: {score:.1f}/4')
            orden.sl_evitar_info = orden.sl_evitar_info or []
            orden.sl_evitar_info.append({'timestamp': datetime.utcnow().
                isoformat(), 'sl': orden.stop_loss, 'precio': precio_cierre})
            return
        resultado = verificar_salida_stoploss(orden.to_dict(), df, config=
            config_actual)
        if resultado.get('cerrar', False):
            if score <= 1 and not evaluar_condiciones_de_cierre_anticipado(
                symbol, df, orden.to_dict(), score, orden.estrategias_activas):
                log.info(
                    f'🛡️ Cierre por SL evitado tras reevaluación técnica: {symbol}'
                    )
                orden.sl_evitar_info = orden.sl_evitar_info or []
                orden.sl_evitar_info.append({'timestamp': datetime.utcnow()
                    .isoformat(), 'sl': orden.stop_loss, 'precio':
                    precio_cierre})
            elif not permitir_cierre_tecnico(symbol, df, precio_cierre,
                orden.to_dict()):
                log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
                orden.sl_evitar_info = orden.sl_evitar_info or []
                orden.sl_evitar_info.append({'timestamp': datetime.utcnow()
                    .isoformat(), 'sl': orden.stop_loss, 'precio':
                    precio_cierre})
            else:
                await trader._cerrar_y_reportar(orden, orden.stop_loss,
                    'Stop Loss', df=df)
        elif resultado.get('evitado', False):
            log.debug('SL evitado correctamente, no se notificará por Telegram'
                )
            metricas_tracker.registrar_sl_evitado()
            orden.sl_evitar_info = orden.sl_evitar_info or []
            orden.sl_evitar_info.append({'timestamp': datetime.utcnow().
                isoformat(), 'sl': orden.stop_loss, 'precio': precio_cierre})
            log.info(
                f"🛡️ SL evitado para {symbol} → {resultado.get('motivo', '')}")
        else:
            log.info(f"ℹ️ {symbol} → {resultado.get('motivo', '')}")
        return
    if precio_max >= orden.take_profit:
        if not getattr(orden, 'parcial_cerrado', False
            ) and orden.cantidad_abierta > 0:
            if trader.es_salida_parcial_valida(orden, orden.take_profit,
                config_actual, df):
                cantidad_parcial = orden.cantidad_abierta * 0.5
                if await trader._cerrar_parcial_y_reportar(orden,
                    cantidad_parcial, orden.take_profit,
                    'Take Profit parcial', df=df):
                    orden.parcial_cerrado = True
                    log.info(
                        '💰 TP parcial alcanzado, se mantiene posición con trailing.'
                        )
            else:
                await trader._cerrar_y_reportar(orden, orden.take_profit,
                    'Take Profit', df=df)
        elif orden.cantidad_abierta > 0:
            await trader._cerrar_y_reportar(orden, orden.take_profit,
                'Take Profit', df=df)
        return
    if orden.cantidad_abierta <= 0:
        return
    if precio_cierre > orden.max_price:
        orden.max_price = precio_cierre
    dinamica = adaptar_configuracion(symbol, df)
    if dinamica:
        config_actual.update(dinamica)
    config_actual = adaptar_configuracion_base(symbol, df, config_actual)
    trader.config_por_simbolo[symbol] = config_actual
    try:
        cerrar, motivo = verificar_trailing_stop(orden.to_dict(),
            precio_cierre, df, config=config_actual)
    except Exception as e:
        log.warning(f'⚠️ Error en trailing stop para {symbol}: {e}')
        cerrar, motivo = False, ''
    if cerrar:
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.
            to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
        elif await trader._cerrar_y_reportar(orden, precio_cierre, motivo,
            df=df):
            log.info(
                f'🔄 Trailing Stop activado para {symbol} a {precio_cierre:.2f}€'
                )
        return
    if verificar_reversion_tendencia(symbol, df, orden.tendencia):
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        if not verificar_filtro_tecnico(symbol, df, orden.
            estrategias_activas, pesos_symbol, config=config_actual):
            nueva_tendencia = trader.estado_tendencia.get(symbol)
            if not nueva_tendencia:
                nueva_tendencia, _ = detectar_tendencia(symbol, df)
                trader.estado_tendencia[symbol] = nueva_tendencia
            if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden
                .to_dict()):
                log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            elif await trader._cerrar_y_reportar(orden, precio_cierre,
                'Cambio de tendencia', tendencia=nueva_tendencia, df=df):
                log.info(
                    f'🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado.'
                    )
            return
    try:
        resultado = evaluar_salidas(orden.to_dict(), df, config=
            config_actual, contexto=contexto)
    except Exception as e:
        log.warning(f'⚠️ Error evaluando salidas para {symbol}: {e}')
        resultado = {}
    if resultado.get('break_even'):
        nuevo_sl = resultado.get('nuevo_sl')
        if nuevo_sl is not None:
            if orden.direccion in ('long', 'compra'):
                if nuevo_sl > orden.stop_loss:
                    orden.stop_loss = nuevo_sl
            elif nuevo_sl < orden.stop_loss:
                orden.stop_loss = nuevo_sl
        orden.break_even_activado = True
        log.info(
            f'🟡 Break-Even activado para {symbol} → SL movido a entrada: {nuevo_sl}'
            )
    if resultado.get('cerrar', False):
        razon = resultado.get('razon', 'Estrategia desconocida')
        tendencia_actual = trader.estado_tendencia.get(symbol)
        if not tendencia_actual:
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            trader.estado_tendencia[symbol] = tendencia_actual
        evaluacion = trader.engine.evaluar_entrada(symbol, df, tendencia=
            tendencia_actual, config=config_actual, pesos_symbol=trader.
            pesos_por_simbolo.get(symbol, {}))
        estrategias = evaluacion.get('estrategias_activas', {})
        puntaje = evaluacion.get('puntaje_total', 0)
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        umbral = calcular_umbral_adaptativo(symbol, df, estrategias,
            pesos_symbol, persistencia=0.0)
        if not validar_necesidad_de_salida(df, orden.to_dict(), estrategias,
            puntaje=puntaje, umbral=umbral, config=config_actual):
            log.info(
                f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas."
                )
            return
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.
            to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            return
        await trader._cerrar_y_reportar(orden, precio_cierre,
            f'Estrategia: {razon}', df=df)
