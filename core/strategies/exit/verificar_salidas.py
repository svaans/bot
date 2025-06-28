from __future__ import annotations


import pandas as pd

from core.utils import configurar_logger
from indicators.atr import calcular_atr
from core.strategies.tendencia import detectar_tendencia
from .supervisor_salidas import SupervisorSalidas
from .salida_por_tendencia import verificar_reversion_tendencia
from .gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from .analisis_previo_salida import permitir_cierre_tecnico
from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.adaptador_dinamico import calcular_umbral_adaptativo


log = configurar_logger("verificar_salidas")

async def verificar_salidas(trader, symbol: str, df: pd.DataFrame) -> None:
    """Evalúa si la orden abierta debe cerrarse."""
    orden = trader.orders.obtener(symbol)
    if not orden:
        log.warning(f"⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}")
        return

    orden.duracion_en_velas = getattr(orden, "duracion_en_velas", 0) + 1

    await trader._piramidar(symbol, orden, df)

    precio_min = float(df["low"].iloc[-1])
    precio_max = float(df["high"].iloc[-1])
    precio_cierre = float(df["close"].iloc[-1])
    config_actual = trader.config_por_simbolo.get(symbol, {})
    log.debug(f"Verificando salidas para {symbol} con orden: {orden.to_dict()}")

    atr = calcular_atr(df)
    volatilidad_rel = atr / precio_cierre if atr and precio_cierre else 1.0
    tendencia_detectada = trader.estado_tendencia.get(symbol)
    if not tendencia_detectada:
        tendencia_detectada, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia_detectada
    contexto = {
        "volatilidad": volatilidad_rel,
        "tendencia": tendencia_detectada,
    }

    supervisor = SupervisorSalidas(orden, config_actual)
    resultado_basico = supervisor.evaluar(
        df,
        precio_cierre=precio_cierre,
        precio_min=precio_min,
        precio_max=precio_max,
    )

    if resultado_basico.get("break_even"):
        orden.break_even_activado = True

    if resultado_basico.get("cerrar"):
        motivo = resultado_basico.get("razon", "")
        if motivo == "Take Profit" and not getattr(orden, "parcial_cerrado", False) and orden.cantidad_abierta > 0:
            if trader.es_salida_parcial_valida(orden, orden.take_profit, config_actual, df):
                cantidad_parcial = orden.cantidad_abierta * 0.5
                if await trader._cerrar_parcial_y_reportar(
                    orden,
                    cantidad_parcial,
                    orden.take_profit,
                    "Take Profit parcial",
                    df=df,
                ):
                    orden.parcial_cerrado = True
                    log.info("💰 TP parcial alcanzado, se mantiene posición con trailing.")
                    return
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
        else:
            await trader._cerrar_y_reportar(orden, precio_cierre, motivo, df=df)
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
                nueva_tendencia, _ = detectar_tendencia(symbol, df)
                trader.estado_tendencia[symbol] = nueva_tendencia
            if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
                log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
            elif await trader._cerrar_y_reportar(
                orden,
                precio_cierre,
                "Cambio de tendencia",
                tendencia=nueva_tendencia,
                df=df,
            ):
                log.info(
                    f"🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado."
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
        umbral = calcular_umbral_adaptativo(
            symbol,
            df,
            estrategias,
            pesos_symbol,
            persistencia=0.0,
        )
        validacion_salida = validar_necesidad_de_salida(
            df,
            orden.to_dict(),
            estrategias,
            puntaje=puntaje,
            umbral=umbral,
            config=config_actual,
        )
        if validacion_salida < 0.5:
            log.info(
                f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas."
            )
            return
        if not permitir_cierre_tecnico(symbol, df, precio_cierre, orden.to_dict()):
            log.info(f"🛡️ Cierre evitado por análisis técnico: {symbol}")
            return
        await trader._cerrar_y_reportar(
            orden, precio_cierre, f"Estrategia: {razon}", df=df
        )
