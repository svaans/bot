"""Coordinador principal de las reglas de salida.

La evaluación sigue una jerarquía estricta para garantizar compatibilidad entre
estrategias y permitir reversión segura:

1. Contexto macroeconómico crítico.
2. Stop Loss y protecciones de emergencia.
3. Take Profit y cierres parciales.
4. Trailing Stop dinámico.
5. Reversiones de tendencia.

Cada etapa valida condiciones técnicas antes de ejecutar el cierre.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc
import asyncio
import pandas as pd
from core.utils import configurar_logger
from core.contexto_externo import obtener_puntaje_contexto
from indicadores.helpers import get_rsi, get_momentum, get_atr
from core.strategies.tendencia import obtener_tendencia
from .salida_stoploss import verificar_salida_stoploss
from .salida_trailing_stop import verificar_trailing_stop
from .salida_por_tendencia import verificar_reversion_tendencia
from .gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from .analisis_previo_salida import permitir_cierre_tecnico, evaluar_condiciones_de_cierre_anticipado
from .analisis_salidas import patron_tecnico_fuerte
from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida
from core.config_manager.dinamica import adaptar_configuracion
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.metricas_semanales import metricas_tracker
from config.exit_defaults import load_exit_config
import time
from core.utils.feature_flags import is_flag_enabled

try:  # pragma: no cover - permite tests sin módulo de métricas
    from core.metrics import registrar_decision
except Exception:  # pragma: no cover
    def registrar_decision(*_args: Any, **_kwargs: Any) -> None:
        return None

log = configurar_logger('verificar_salidas')


def _metrics_extended_enabled() -> bool:
    return is_flag_enabled("metrics.extended.enabled")


async def _chequear_contexto_macro(trader, orden, df) -> bool:
    """Comprueba el puntaje macroeconómico y cierra la posición si supera el
    umbral configurado. Esta validación tiene la prioridad más alta."""

    symbol = orden.symbol
    # Se valida contexto macroeconómico antes de tomar decisiones de cierre.
    puntaje_macro = obtener_puntaje_contexto(symbol)
    cfg = load_exit_config(symbol)
    if abs(puntaje_macro) > cfg['umbral_puntaje_macro_cierre']:
        log.info(f'[{symbol}] Contexto macro crítico ({puntaje_macro:.2f}). Se prioriza cierre.')
        await trader._cerrar_y_reportar(orden, float(df["close"].iloc[-1]), 'Contexto macro', df=df)
        return True
    return False


async def _manejar_stop_loss(trader, orden, df) -> bool:
    """Gestiona la lógica de Stop Loss. Se ejecuta después de validar el
    contexto macro."""

    symbol = orden.symbol
    precio_min = float(df['low'].iloc[-1])
    precio_cierre = float(df['close'].iloc[-1])
    sl_emergencia = getattr(orden, 'sl_emergencia', None)
    if sl_emergencia is not None:
        cond_emergencia = (
            orden.direccion in ('long', 'compra') and precio_min <= sl_emergencia
        ) or (
            orden.direccion in ('short', 'venta') and precio_min >= sl_emergencia
        )
        if cond_emergencia:
            await trader._cerrar_y_reportar(
                orden, precio_cierre, 'Stop Loss de emergencia', df=df
            )
            return True
    if precio_min > orden.stop_loss:
        return False
    config_actual = trader.config_por_simbolo.get(
        symbol, load_exit_config(symbol)
    )
    resultado = await verificar_salida_stoploss(
        orden.to_dict(), df, config=config_actual
    )

    def establecer_sl_emergencia():
        extra = config_actual.get('sl_emergency_pct', 0.0)
        if extra > 0 and getattr(orden, 'sl_emergencia', None) is None:
            if orden.direccion in ('long', 'compra'):
                orden.sl_emergencia = orden.stop_loss * (1 - extra)
            else:
                orden.sl_emergencia = orden.stop_loss * (1 + extra)
            log.info(
                f'⚠️ SL de emergencia establecido en {orden.sl_emergencia:.2f}'
            )
    if resultado.get('cerrar'):
        if not permitir_cierre_tecnico(
            symbol, df, precio_cierre, orden.to_dict()
        ):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            metricas_tracker.registrar_sl_evitado()
            orden.sl_evitar_info = orden.sl_evitar_info or []
            orden.sl_evitar_info.append(
                {
                    'timestamp': datetime.now(UTC).isoformat(),
                    'sl': orden.stop_loss,
                    'precio': precio_cierre,
                }
            )
            establecer_sl_emergencia()
            if getattr(orden, 'sl_emergencia', None) is not None:
                cond = (
                    orden.direccion in ('long', 'compra')
                    and precio_min <= orden.sl_emergencia
                ) or (
                    orden.direccion in ('short', 'venta')
                    and precio_min >= orden.sl_emergencia
                )
                if cond:
                    await trader._cerrar_y_reportar(
                        orden,
                        precio_cierre,
                        'Stop Loss de emergencia',
                        df=df,
                    )
                    return True
            return False
        await trader._cerrar_y_reportar(orden, precio_cierre, 'Stop Loss', df=df)
        return True
    if resultado.get('evitado'):
        metricas_tracker.registrar_sl_evitado()
        orden.sl_evitar_info = orden.sl_evitar_info or []
        orden.sl_evitar_info.append(
            {
                'timestamp': datetime.now(UTC).isoformat(),
                'sl': orden.stop_loss,
                'precio': precio_cierre,
            }
        )
        log.info(
            f"🛡️ SL evitado para {symbol} → {resultado.get('motivo', '')}"
        )
        establecer_sl_emergencia()
        if getattr(orden, 'sl_emergencia', None) is not None:
            cond = (
                orden.direccion in ('long', 'compra')
                and precio_min <= orden.sl_emergencia
            ) or (
                orden.direccion in ('short', 'venta')
                and precio_min >= orden.sl_emergencia
            )
            if cond:
                await trader._cerrar_y_reportar(
                    orden, precio_cierre, 'Stop Loss de emergencia', df=df
                )
                return True
    else:
        log.info(f"ℹ️ {symbol} → {resultado.get('motivo', '')}")
    return False


async def _procesar_take_profit(trader, orden, df) -> bool:
    """Verifica si se alcanzó el Take Profit y realiza cierres parciales o
    totales. Tiene prioridad después del Stop Loss."""

    symbol = orden.symbol
    precio_max = float(df['high'].iloc[-1])
    if precio_max < orden.take_profit:
        return False
    precio_cierre = float(df['close'].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, load_exit_config(symbol))
    if not getattr(orden, 'parcial_cerrado', False) and orden.cantidad_abierta > 0:
        if await trader.es_salida_parcial_valida(orden, orden.take_profit, config_actual, df):
            cantidad_parcial = orden.cantidad_abierta * 0.5
            if await trader._cerrar_parcial_y_reportar(orden, cantidad_parcial, orden.take_profit, 'Take Profit parcial', df=df):
                orden.parcial_cerrado = True
                log.info('💰 TP parcial alcanzado, se mantiene posición con trailing.')
                return False
        else:
            await trader._cerrar_y_reportar(orden, orden.take_profit, 'Take Profit', df=df)
            return True
    elif orden.cantidad_abierta > 0:
        await trader._cerrar_y_reportar(orden, orden.take_profit, 'Take Profit', df=df)
        return True
    return False


async def _manejar_trailing_stop(trader, orden, df) -> bool:
    """Aplica el trailing stop y gestiona ajustes dinámicos. Se ejecuta tras
    procesar el Take Profit."""

    symbol = orden.symbol
    precio_cierre = float(df['close'].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, load_exit_config(symbol))
    if precio_cierre > orden.max_price:
        orden.max_price = precio_cierre
    config_actual = adaptar_configuracion(symbol, df, config_actual)
    trader.config_por_simbolo[symbol] = config_actual
    try:
        cerrar, motivo = verificar_trailing_stop(
            orden.to_dict(), precio_cierre, df, config=config_actual
        )
    except (KeyError, ValueError, TypeError) as e:
        log.warning(f'⚠️ Error en trailing stop para {symbol}: {e}')
        cerrar, motivo = False, ''
    if cerrar:
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            return False
        if await trader._cerrar_y_reportar(orden, precio_cierre, motivo, df=df):
            spread = None
            if 'spread' in df.columns:
                spread = df['spread'].iloc[-1] / precio_cierre
            cfg = trader.config_por_simbolo.get(symbol, load_exit_config(symbol))
            if spread and spread > cfg['max_spread_ratio']:
                orden.intentos_cierre += 1
                if orden.intentos_cierre >= cfg['max_intentos_cierre']:
                    await trader._cerrar_y_reportar(orden, precio_cierre, 'Cierre forzado por spread', df=df)
                    return True
                delay = cfg['delay_reintento_cierre']
                log.info(f'[{symbol}] Spread {spread:.4f} demasiado alto, reintento {orden.intentos_cierre} en {delay}s')
                await asyncio.sleep(delay)
                return False
            log.info(f'🔄 Trailing Stop activado para {symbol} a {precio_cierre:.2f}€')
            return True
    return False


async def _manejar_cambio_tendencia(trader, orden, df) -> bool:
    """Detecta reversiones de tendencia y cierra la posición si procede. Se
    evalúa después del trailing stop."""

    symbol = orden.symbol
    config_actual = trader.config_por_simbolo.get(symbol, load_exit_config(symbol))
    if not verificar_reversion_tendencia(symbol, df, orden.tendencia):
        orden.reversion_tendencia_contador = 0
        return False

    # Contador de reversiones consecutivas
    orden.reversion_tendencia_contador = getattr(
        orden, 'reversion_tendencia_contador', 0
    ) + 1

    max_velas = config_actual.get('max_velas_reversion_tendencia', 3)
    if max_velas and orden.reversion_tendencia_contador >= max_velas:
        nueva_tendencia = obtener_tendencia(symbol, df)
        precio_cierre = float(df['close'].iloc[-1])
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            return False
        if await trader._cerrar_y_reportar(
            orden,
            precio_cierre,
            'Cambio de tendencia forzado',
            tendencia=nueva_tendencia,
            df=df,
        ):
            log.info(
                f'🔄 Cambio de tendencia persistente para {symbol}. Cierre forzado.'
            )
            orden.reversion_tendencia_contador = 0
            return True
        return False
        
    pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
    if not await verificar_filtro_tecnico(
        symbol, df, orden.estrategias_activas, pesos_symbol, config=config_actual
    ):
        nueva_tendencia = obtener_tendencia(symbol, df)
        precio_cierre = float(df['close'].iloc[-1])
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            return False
        if await trader._cerrar_y_reportar(orden, precio_cierre, 'Cambio de tendencia', tendencia=nueva_tendencia, df=df):
            log.info(f'🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado.')
            return True
    return False


async def _aplicar_salidas_adicionales(trader, orden, df) -> bool:
    """Evalúa estrategias complementarias de salida. Se ejecuta en última
    instancia."""

    symbol = orden.symbol
    precio_cierre = float(df['close'].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, load_exit_config(symbol))
    atr = get_atr(df)
    volatilidad_rel = atr / precio_cierre if atr and precio_cierre else 1.0
    tendencia_detectada = obtener_tendencia(symbol, df)
    contexto = {'volatilidad': volatilidad_rel, 'tendencia': tendencia_detectada}
    try:
        resultado = await evaluar_salidas(
            orden.to_dict(), df, config=config_actual, contexto=contexto
        )
    except (KeyError, ValueError, TypeError) as e:
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
        log.info(f'🟡 Break-Even activado para {symbol} → SL movido a entrada: {nuevo_sl}')
    if resultado.get('cerrar', False):
        razon = resultado.get('razon', 'Estrategia desconocida')
        tendencia_actual = obtener_tendencia(symbol, df)
        evaluacion = await trader.engine.evaluar_entrada(
            symbol,
            df,
            tendencia=tendencia_actual,
            config=config_actual,
            pesos_symbol=trader.pesos_por_simbolo.get(symbol, {}),
        )
        estrategias = evaluacion.get('estrategias_activas', {})
        puntaje = evaluacion.get('puntaje_total', 0)
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        umbral = calcular_umbral_adaptativo(symbol, df)
        if not validar_necesidad_de_salida(df, orden.to_dict(), estrategias, puntaje=puntaje, umbral=umbral, config=config_actual):
            log.info(f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas.")
            return False
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f'🛡️ Cierre evitado por análisis técnico: {symbol}')
            return False
        await trader._cerrar_y_reportar(orden, precio_cierre, f'Estrategia: {razon}', df=df)
        return True
    return False


async def verificar_salidas(trader, symbol: str, df: pd.DataFrame) -> None:
    """Evalúa si la orden abierta debe cerrarse."""

    decision_action = "exit_skip"
    try:
        inicio_total = time.time()
        trader.config_por_simbolo[symbol] = load_exit_config(symbol)
        orden = trader.orders.obtener(symbol)
        if not orden:
            log.warning(f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
            return

        decision_action = "exit_hold"
        orden.duracion_en_velas = getattr(orden, 'duracion_en_velas', 0) + 1
        await trader._piramidar(symbol, orden, df)

        timeout = trader.config_por_simbolo[symbol].get('timeout_validaciones', 5)

        tareas_principales = [
            _chequear_contexto_macro(trader, orden, df),
            _manejar_stop_loss(trader, orden, df),
            _procesar_take_profit(trader, orden, df),
        ]

        try:
            resultados = await asyncio.wait_for(
                asyncio.gather(*tareas_principales, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning(f'Timeout en verificaciones principales para {symbol}')
            decision_action = "exit_timeout"
            return

        for resultado in resultados:
            if isinstance(resultado, Exception):
                log.error(f'Error en verificación principal para {symbol}: {resultado}')
                if decision_action != "exit_closed":
                    decision_action = "exit_error"
            elif resultado:
                log.debug(f'Validación principal activó cierre para {symbol}')
                decision_action = "exit_closed"
                return

        if orden.cantidad_abierta <= 0:
            log.debug(
                f'Posición cerrada tras verificaciones principales en {symbol} en {time.time() - inicio_total:.2f}s'
            )
            decision_action = "exit_closed"
            return
        tareas_secundarias = [
            _manejar_trailing_stop(trader, orden, df),
            _manejar_cambio_tendencia(trader, orden, df),
            _aplicar_salidas_adicionales(trader, orden, df),
        ]

        try:
            resultados = await asyncio.wait_for(
                asyncio.gather(*tareas_secundarias, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning(f'Timeout en verificaciones secundarias para {symbol}')
            decision_action = "exit_timeout"
            return
        
        for resultado in resultados:
            if isinstance(resultado, Exception):
                log.error(f'Error en verificación secundaria para {symbol}: {resultado}')
                if decision_action != "exit_closed":
                    decision_action = "exit_error"
            elif resultado:
                log.debug(f'Validación secundaria activó cierre para {symbol}')
                decision_action = "exit_closed"
                return
        log.debug(f'verificar_salidas total {time.time() - inicio_total:.2f}s para {symbol}')
    finally:
        if _metrics_extended_enabled():
            registrar_decision(symbol, decision_action)