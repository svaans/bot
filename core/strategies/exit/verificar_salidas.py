from __future__ import annotations


import pandas as pd
import asyncio
from time import perf_counter
import traceback

from core.async_utils import dump_tasks_stacktraces

from core.utils import configurar_logger
from core.registro_metrico import registro_metrico
from indicators.atr import calcular_atr
from core.strategies.tendencia import detectar_tendencia
from .salida_por_tendencia import verificar_reversion_tendencia
from .gestor_salidas import (
    evaluar_salidas,
    verificar_filtro_tecnico,
)
from .salida_trailing_stop import verificar_trailing_stop
from .salida_break_even import salida_break_even
from .salida_stoploss import verificar_salida_stoploss
from .analisis_previo_salida import permitir_cierre_tecnico
from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.adaptador_dinamico import calcular_umbral_adaptativo

from core.async_utils import run_with_timeout


log = configurar_logger("verificar_salidas")

async def verificar_salidas(trader, symbol: str, df: pd.DataFrame) -> None:
    """Evalúa si la orden abierta debe cerrarse."""
    inicio = perf_counter()
    try:
        await asyncio.wait_for(
            _verificar_salidas_impl(trader, symbol, df), timeout=20
        )
    except asyncio.TimeoutError:
        log.error(f"⏱️ Timeout verificando salidas de {symbol}")
        log.error("Tareas:\n%s", dump_tasks_stacktraces(asyncio.all_tasks()))
        log.error("Stack actual:\n%s", "".join(traceback.format_stack()))
        return
    finally:
        dur = (perf_counter() - inicio) * 1000.0
        registro_metrico.registrar("verif_salidas", {"symbol": symbol, "ms": dur})
        log.debug(f"[{symbol}] verificar_salidas completado en {dur:.2f} ms")


async def _verificar_salidas_impl(trader, symbol: str, df: pd.DataFrame) -> None:
    """Implementación interna de :func:`verificar_salidas`."""

    if df is None or df.empty:
        log.warning(f"🚫 [{symbol}] DataFrame vacío. Se aborta verificación de salidas")
        return
    
    log.debug(f"[{symbol}] tamaño DataFrame: {len(df)}")
    inicio = perf_counter()
    orden = trader.orders.obtener(symbol)
    if not orden:
        log.warning(f"⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}")
        return
    
    # Fragmenta el DataFrame para ceder control al event loop
    for i in range(0, len(df), 200):
        df.iloc[i : i + 200]
        await asyncio.sleep(0)
    orden.duracion_en_velas = getattr(orden, "duracion_en_velas", 0) + 1

    await run_with_timeout(
        trader._piramidar(symbol, orden, df),
        5,
        logger=log,
        name=f"piramidar {symbol}",
    )

    precio_min = float(df["low"].iloc[-1])
    precio_max = float(df["high"].iloc[-1])
    precio_cierre = float(df["close"].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, {})
    log.debug(f"Verificando salidas para {symbol} con orden: {orden.to_dict()}")

    atr = await asyncio.to_thread(calcular_atr, df)
    volatilidad_rel = atr / precio_cierre if atr and precio_cierre else 1.0
    tendencia_detectada = trader.estado_tendencia.get(symbol)
    if not tendencia_detectada:
        tendencia_detectada, _ = await asyncio.to_thread(detectar_tendencia, symbol, df)
        trader.estado_tendencia[symbol] = tendencia_detectada
    contexto = {
        "volatilidad": volatilidad_rel,
        "tendencia": tendencia_detectada,
    }

    # --- Validación de parámetros ---
    if any(v < 0 for v in [orden.stop_loss, orden.take_profit, orden.precio_entrada]):
        log.error(f"[{symbol}] Valores incoherentes en la orden: {orden.to_dict()}")
        return

    # === 1. Stop Loss ===
    try:
        res_sl = await asyncio.wait_for(
            asyncio.to_thread(verificar_salida_stoploss, orden.to_dict(), df, config_actual),
            timeout=config_actual.get("timeout_sl", 3),
        )
    except asyncio.TimeoutError:
        log.error(f"⏱️ Timeout SL {symbol}")
        res_sl = {"cerrar": False}
    except Exception as e:  # noqa: BLE001
        log.error(f"Error evaluando SL {symbol}: {e}")
        res_sl = {"cerrar": False}
    if res_sl.get("cerrar"):
        motivo_sl = res_sl.get("motivo", "Stop Loss")
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f"🛡️ Cierre SL evitado por análisis técnico: {symbol}")
        else:
            await run_with_timeout(
                trader._cerrar_y_reportar(orden, precio_cierre, motivo_sl, df=df),
                10,
                logger=log,
                name=f"cerrar SL {symbol}",
            )
        return

    await asyncio.sleep(0)

    # === 2. Trailing Stop ===
    try:
        cerrar_ts, motivo_ts = await asyncio.wait_for(
            asyncio.to_thread(
                verificar_trailing_stop, orden.to_dict(), precio_cierre, df, config_actual
            ),
            timeout=config_actual.get("timeout_trailing", 3),
        )
    except asyncio.TimeoutError:
        log.error(f"⏱️ Timeout trailing {symbol}")
        cerrar_ts, motivo_ts = False, ""
    except Exception as e:  # noqa: BLE001
        log.error(f"Error en trailing stop {symbol}: {e}")
        cerrar_ts, motivo_ts = False, ""
    if cerrar_ts:
        await run_with_timeout(
            trader._cerrar_y_reportar(orden, precio_cierre, motivo_ts, df=df),
            10,
            logger=log,
            name=f"cerrar trailing {symbol}",
        )
        return

    await asyncio.sleep(0)

    # === 3. Break Even ===
    try:
        res_be = await asyncio.wait_for(
            asyncio.to_thread(salida_break_even, orden.to_dict(), df, config_actual),
            timeout=config_actual.get("timeout_break_even", 3),
        )
    except asyncio.TimeoutError:
        log.error(f"⏱️ Timeout break-even {symbol}")
        res_be = {}
    except Exception as e:  # noqa: BLE001
        log.error(f"Error en break-even {symbol}: {e}")
        res_be = {}

    if res_be.get("break_even"):
        nuevo_sl = res_be.get("nuevo_sl")
        if nuevo_sl is not None:
            if orden.direccion in ("long", "compra"):
                if nuevo_sl > orden.stop_loss:
                    orden.stop_loss = nuevo_sl
            else:
                if nuevo_sl < orden.stop_loss:
                    orden.stop_loss = nuevo_sl
        orden.break_even_activado = True
        log.info(f"🟡 Break-Even activado para {symbol} → SL movido a entrada: {nuevo_sl}")

    # === 4. Condiciones técnicas (TP, tendencia, estrategias) ===
    alcanzado_tp = (
        (orden.direccion in ("long", "compra") and precio_max >= orden.take_profit)
        or (orden.direccion in ("short", "venta") and precio_min <= orden.take_profit)
    )
    if alcanzado_tp:
        if not getattr(orden, "parcial_cerrado", False) and orden.cantidad_abierta > 0:
            if trader.es_salida_parcial_valida(orden, orden.take_profit, config_actual, df):
                fraccion = trader.calcular_fraccion_parcial(orden, df, config_actual)
                cantidad_parcial = orden.cantidad_abierta * fraccion
                cerrada = await run_with_timeout(
                    trader._cerrar_parcial_y_reportar(
                        orden,
                        cantidad_parcial,
                        orden.take_profit,
                        "Take Profit parcial",
                        df=df,
                    ),
                    10,
                    logger=log,
                    name=f"cierre parcial {symbol}",
                )
                if cerrada:
                    orden.parcial_cerrado = True
                    log.info(
                        "💰 TP parcial alcanzado, se mantiene posición con trailing."
                    )
                    dur = (perf_counter() - inicio) * 1000.0
                    registro_metrico.registrar(
                        "verif_salidas", {"symbol": symbol, "ms": dur}
                    )
                    log.debug(
                        f"[{symbol}] verificar_salidas TP parcial tomó {dur:.2f} ms"
                    )
                    return
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
        else:
            await run_with_timeout(
                trader._cerrar_y_reportar(orden, precio_cierre, "Take Profit", df=df),
                10,
                logger=log,
                name=f"cerrar orden {symbol}",
            )
        return

    # --- Cambio de tendencia ---
    velas_confirm = config_actual.get("velas_confirmacion_reversion", 2)
    if verificar_reversion_tendencia(symbol, df, orden.tendencia, velas=velas_confirm):
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        if not verificar_filtro_tecnico(
            symbol, df, orden.estrategias_activas, pesos_symbol, config=config_actual
        ):
            nueva_tendencia = trader.estado_tendencia.get(symbol)
            if not nueva_tendencia:
                nueva_tendencia, _ = await asyncio.to_thread(detectar_tendencia, symbol, df)
                trader.estado_tendencia[symbol] = nueva_tendencia
            if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
                log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
            else:
                cerrado = await run_with_timeout(
                    trader._cerrar_y_reportar(
                        orden,
                        precio_cierre,
                        "Cambio de tendencia",
                        tendencia=nueva_tendencia,
                        df=df,
                    ),
                    10,
                    logger=log,
                    name=f"cierre tendencia {symbol}",
                )
                if cerrado:
                    log.info(
                        f"🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado."
                    )
            dur = (perf_counter() - inicio) * 1000.0
            registro_metrico.registrar(
                "verif_salidas", {"symbol": symbol, "ms": dur}
            )
            log.debug(
                f"[{symbol}] verificar_salidas cambio tendencia tomó {dur:.2f} ms"
            )
            return

    # --- Estrategias de salida personalizadas ---
    try:
        resultado = evaluar_salidas(
            orden.to_dict(),
            df,
            config=config_actual,
            contexto=contexto,
        )
    except Exception as e:
        log.warning(f"⚠️ Error evaluando salidas para {symbol}: {e}")
        resultado = {}

    if resultado.get("break_even"):
        nuevo_sl = resultado.get("nuevo_sl")
        if nuevo_sl is not None:
            if orden.direccion in ("long", "compra"):
                if nuevo_sl > orden.stop_loss:
                    orden.stop_loss = nuevo_sl
            else:
                if nuevo_sl < orden.stop_loss:
                    orden.stop_loss = nuevo_sl
        orden.break_even_activado = True
        log.info(
            f"🟡 Break-Even activado para {symbol} → SL movido a entrada: {nuevo_sl}"
        )

    if resultado.get("cerrar", False):
        razon = resultado.get("razon", "Estrategia desconocida")
        tendencia_actual = trader.estado_tendencia.get(symbol)
        if not tendencia_actual:
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            trader.estado_tendencia[symbol] = tendencia_actual
        evaluacion = trader.engine.evaluar_entrada(
            symbol,
            df,
            tendencia=tendencia_actual,
            config=config_actual,
            pesos_symbol=trader.pesos_por_simbolo.get(symbol, {}),
        )
        estrategias = evaluacion.get("estrategias_activas", {})
        puntaje = evaluacion.get("puntaje_total", 0)
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        t_eval = perf_counter()
        umbral = await asyncio.to_thread(
            calcular_umbral_adaptativo,
            symbol,
            df,
            estrategias,
            pesos_symbol,
            persistencia=0.0,
        )
        validacion_salida = await asyncio.to_thread(
            validar_necesidad_de_salida,
            df,
            orden.to_dict(),
            estrategias,
            puntaje=puntaje,
            umbral=umbral,
            config=config_actual,
        )
        dur_eval = (perf_counter() - t_eval) * 1000.0
        log.debug(f"[{symbol}] Validación técnica tomó {dur_eval:.2f} ms")
        await asyncio.sleep(0)
        if validacion_salida < 0.5:
            log.info(
                f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas."
            )
            dur = (perf_counter() - inicio) * 1000.0
            registro_metrico.registrar(
                "verif_salidas", {"symbol": symbol, "ms": dur}
            )
            log.debug(f"[{symbol}] verificar_salidas rechazo técnico tomó {dur:.2f} ms")
            return
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
            dur = (perf_counter() - inicio) * 1000.0
            registro_metrico.registrar(
                "verif_salidas", {"symbol": symbol, "ms": dur}
            )
            log.debug(f"[{symbol}] verificar_salidas bloqueo técnico tomó {dur:.2f} ms")
            return
        await run_with_timeout(
            trader._cerrar_y_reportar(
                orden,
                precio_cierre,
                f"Estrategia: {razon}",
                df=df,
            ),
            10,
            logger=log,
            name=f"cierre estrategia {symbol}",
        )
