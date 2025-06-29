from __future__ import annotations

from datetime import datetime, timezone
import pandas as pd

from core.utils import configurar_logger
from time import perf_counter
from core.registro_metrico import registro_metrico
from core.adaptador_dinamico import (
    calcular_umbral_adaptativo,
    calcular_tp_sl_adaptativos,
)
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.data import coincidencia_parcial
from core.estrategias import filtrar_por_direccion
from core.strategies.tendencia import (
    detectar_tendencia,
    obtener_parametros_persistencia,
)
from core.strategies.evaluador_tecnico import (
    evaluar_puntaje_tecnico,
    calcular_umbral_adaptativo as calc_umbral_tecnico,
    cargar_pesos_tecnicos,
)
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope
from core.utils.utils import distancia_minima_valida, validar_ratio_beneficio
from core.strategies.entry.validadores import validar_spread
import asyncio

log = configurar_logger("verificar_entrada")

async def verificar_entrada(
    trader, symbol: str, df: pd.DataFrame, estado
) -> dict | None:
    """Evalúa las condiciones de entrada y devuelve info de la operación."""
    inicio = perf_counter()
    try:
        return await asyncio.wait_for(
            _verificar_entrada_impl(trader, symbol, df, estado), timeout=30
        )
    except asyncio.TimeoutError:
        log.warning(f"⏱️ Timeout interno en verificación de entrada para {symbol}")
        return None
    finally:
        dur = (perf_counter() - inicio) * 1000.0
        registro_metrico.registrar("verif_entrada", {"symbol": symbol, "ms": dur})
        log.debug(f"[{symbol}] verificar_entrada completado en {dur:.2f} ms")

async def _verificar_entrada_impl(
    trader, symbol: str, df: pd.DataFrame, estado
) -> dict | None:
    log.debug(f"⏳ Empezando verificación {symbol}")
    if df is None or df.empty:
        log.warning(f"🚫 [{symbol}] DataFrame vacío. Se aborta la evaluación")
        return None
    config_actual = trader.config_por_simbolo.get(symbol, {})
    t_cfg = perf_counter()
    dinamica = adaptar_configuracion(symbol, df)
    if dinamica:
        config_actual.update(dinamica)
    config_actual = adaptar_configuracion_base(symbol, df, config_actual)
    dur_cfg = (perf_counter() - t_cfg) * 1000.0
    log.debug(f"[{symbol}] adaptar_config tomó {dur_cfg:.2f} ms")
    max_spread = config_actual.get("max_spread", 0.002)
    spread_conf = validar_spread(df, max_spread)
    if spread_conf <= 0:
        alto = float(df["high"].iloc[-1])
        bajo = float(df["low"].iloc[-1])
        cierre = float(df["close"].iloc[-1]) or 1.0
        spread = (alto - bajo) / cierre
        log.warning(
            f"🚫 [{symbol}] Spread {spread:.4f} supera umbral {max_spread:.4f}."
        )
        return None
    log.debug(
        f"[{symbol}] 🔒 solicitando state_lock config @ {datetime.now(timezone.utc).isoformat()}"
    )
    async with trader.state_lock:
        log.debug(
            f"[{symbol}] ✅ state_lock config adquirido @ {datetime.now(timezone.utc).isoformat()}"
        )
        trader.config_por_simbolo[symbol] = config_actual
        if hasattr(trader, "capital_manager"):
            trader.capital_manager.set_riesgo_maximo(
                symbol, config_actual.get("riesgo_maximo_diario", 1.0)
            )
    log.debug(
        f"[{symbol}] 🔓 state_lock config liberado @ {datetime.now(timezone.utc).isoformat()}"
    )

    log.debug(
        f"[{symbol}] 🔒 solicitando state_lock tendencia @ {datetime.now(timezone.utc).isoformat()}"
    )

    async with trader.state_lock:
        log.debug(
            f"[{symbol}] ✅ state_lock tendencia adquirido @ {datetime.now(timezone.utc).isoformat()}"
        )
        tendencia_actual = trader.estado_tendencia.get(symbol)
    log.debug(
        f"[{symbol}] 🔓 state_lock tendencia liberado @ {datetime.now(timezone.utc).isoformat()}"
    )
        
    if not tendencia_actual:
        tendencia_actual, _ = detectar_tendencia(symbol, df)
        log.debug(
            f"[{symbol}] 🔒 solicitando state_lock set tendencia @ {datetime.now(timezone.utc).isoformat()}"
        )
        async with trader.state_lock:
            log.debug(
                f"[{symbol}] ✅ state_lock set tendencia adquirido @ {datetime.now(timezone.utc).isoformat()}"
            )
            trader.estado_tendencia[symbol] = tendencia_actual
        log.debug(
            f"[{symbol}] 🔓 state_lock set tendencia liberado @ {datetime.now(timezone.utc).isoformat()}"
        )
    log.debug(f"[{symbol}] Tendencia detectada: {tendencia_actual}")

    volatilidad_actual = df["close"].pct_change().tail(20).std()
    trader.persistencia.ajustar_minimo(symbol, volatilidad_actual)

    t_engine = perf_counter()
    evaluacion = trader.engine.evaluar_entrada(
        symbol,
        df,
        tendencia=tendencia_actual,
        config=config_actual,
        pesos_symbol=trader.pesos_por_simbolo.get(symbol, {}),
    )
    dur_engine = (perf_counter() - t_engine) * 1000.0
    log.debug(f"[{symbol}] engine tomó {dur_engine:.2f} ms")
    estrategias = evaluacion.get("estrategias_activas", {})
    log.debug(f"[{symbol}] Estrategias iniciales desde engine: {estrategias}")
    if not estrategias:
        log.warning(f"⚠️ [{symbol}] Sin estrategias activas tras evaluación. Tendencia detectada previamente.")
    else:
        log.info(f"🧪 [{symbol}] Estrategias activas: {list(estrategias.keys())}")

    if not evaluacion.get("permitido", True):
        motivo = evaluacion.get("motivo_rechazo", "desconocido")
        log.info(f"🚫 [{symbol}] Engine rechazó la entrada por: {motivo}")
        return None
    
    estado.buffer[-1]["estrategias_activas"] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)

    pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
    peso_max = sum(pesos_symbol.values()) or 1.0
    peso_minimo, min_estrategias = obtener_parametros_persistencia(
        tendencia_actual, volatilidad_actual
    )

    if len(estado.buffer) < 30:
        persistencia = await asyncio.to_thread(
            coincidencia_parcial, estado.buffer, pesos_symbol, ventanas=5
        )
        log.debug(
            f"[{symbol}] Persistencia parcial (buffer corto): {persistencia:.2f}"
        )
        if persistencia < peso_minimo * peso_max:
            return None

    persistencia_score = await asyncio.to_thread(
        coincidencia_parcial, estado.buffer, pesos_symbol, ventanas=5
    )
    umbral = calcular_umbral_adaptativo(
        symbol,
        df,
        estrategias,
        pesos_symbol,
        persistencia=persistencia_score,
    )

    estrategias_persistentes: dict[str, bool] = {}
    for idx, (e, act) in enumerate(estrategias.items()):
        if act and trader.persistencia.es_persistente(symbol, e):
            estrategias_persistentes[e] = True
        if idx % 5 == 0:
            await asyncio.sleep(0)
    log.debug(f"[{symbol}] Estrategias persistentes: {estrategias_persistentes}")

    peso_persistente = sum(pesos_symbol.get(k, 0.0) for k in estrategias_persistentes)
    if (
        len(estrategias_persistentes) < min_estrategias
        or peso_persistente < peso_minimo * peso_max
    ):
        log.warning(
            f"[{symbol}] Persistencia insuficiente: {len(estrategias_persistentes)} < {min_estrategias} "
            f"o peso {peso_persistente:.2f} < {peso_minimo * peso_max:.2f}"
        )
        return None

    if not estrategias_persistentes:
        log.warning(f"[{symbol}] Ninguna estrategia pasó el filtro de persistencia.")
        return None

    direccion = "short" if tendencia_actual == "bajista" else "long"
    estrategias_persistentes, incoherentes = filtrar_por_direccion(
        estrategias_persistentes, direccion
    )
    log.debug(
        f"[{symbol}] Después del filtro por dirección ({direccion}): {estrategias_persistentes}"
    )
    log.debug(f"[{symbol}] Estrategias incoherentes: {incoherentes}")

    if not estrategias_persistentes:
        log.warning(f"[{symbol}] Estrategias incoherentes con la dirección {direccion}.")
        return None

    penalizacion = 0.05 * (len(incoherentes) ** 2) if incoherentes else 0.0
    puntaje = sum(pesos_symbol.get(k, 0) for k in estrategias_persistentes)
    puntaje += trader.persistencia.peso_extra * len(estrategias_persistentes)
    puntaje -= penalizacion
    puntaje *= spread_conf
    estado.ultimo_umbral = umbral
    log.debug(
        f"[{symbol}] Puntaje preliminar {puntaje:.2f} "
        f"(penalización {penalizacion:.2f}, spread {spread_conf:.2f})"
    )

    log.debug(
        f"[{symbol}] 🔒 solicitando state_lock cierre @ {datetime.now(timezone.utc).isoformat()}"
    )

    async with trader.state_lock:
        log.debug(
            f"[{symbol}] ✅ state_lock cierre adquirido @ {datetime.now(timezone.utc).isoformat()}"
        )
        cierre = trader.historial_cierres.get(symbol)
    log.debug(
        f"[{symbol}] 🔓 state_lock cierre liberado @ {datetime.now(timezone.utc).isoformat()}"
    )
    if cierre:
        motivo = cierre.get("motivo")
        if motivo == "stop loss":
            cooldown_velas = int(config_actual.get("cooldown_tras_perdida", 5))
            velas = cierre.get("velas", 0)
            if velas < cooldown_velas:
                cierre["velas"] = velas + 1
                restante = cooldown_velas - velas
                log.info(f"🕒 [{symbol}] Cooldown activo por stop loss. Quedan {restante} velas.")
                return None
            else:
                log.debug(
                    f"[{symbol}] 🔒 solicitando state_lock borrar cierre @ {datetime.now(timezone.utc).isoformat()}"
                )
                async with trader.state_lock:
                    log.debug(
                        f"[{symbol}] ✅ state_lock borrar cierre adquirido @ {datetime.now(timezone.utc).isoformat()}"
                    )
                    trader.historial_cierres.pop(symbol, None)
                log.debug(
                    f"[{symbol}] 🔓 state_lock borrar cierre liberado @ {datetime.now(timezone.utc).isoformat()}"
                )
        elif motivo == "cambio de tendencia":
            precio_actual = float(df["close"].iloc[-1])
            if not trader._validar_reentrada_tendencia(symbol, df, cierre, precio_actual):
                cierre["velas"] = cierre.get("velas", 0) + 1
                log.info(f"🚫 [{symbol}] Reentrada bloqueada por cambio de tendencia.")
                return None
            else:
                log.debug(
                    f"[{symbol}] 🔒 solicitando state_lock borrar cierre @ {datetime.now(timezone.utc).isoformat()}"
                )
                async with trader.state_lock:
                    log.debug(
                        f"[{symbol}] ✅ state_lock borrar cierre adquirido @ {datetime.now(timezone.utc).isoformat()}"
                    )
                    trader.historial_cierres.pop(symbol, None)
                log.debug(
                    f"[{symbol}] 🔓 state_lock borrar cierre liberado @ {datetime.now(timezone.utc).isoformat()}"
                )
    registro = cierre or {}
    fecha_hoy = datetime.now(timezone.utc).date().isoformat()
    if (
        registro.get("fecha_perdidas") == fecha_hoy
        and registro.get("perdidas_consecutivas", 0) >= 6
    ):
        log.info(f"🚫 [{symbol}] Bloqueo por pérdidas consecutivas en el día.")
        return None

    estrategias_activas: dict[str, float] = {}
    for idx, e in enumerate(estrategias_persistentes):
        estrategias_activas[e] = pesos_symbol.get(e, 0.0)
        if idx % 5 == 0:
            await asyncio.sleep(0)
    peso_total = sum(estrategias_activas.values())
    persistencia = await asyncio.to_thread(
        coincidencia_parcial, estado.buffer, pesos_symbol, ventanas=5
    )

    log.info(
        f"📊 [{symbol}] Puntaje: {puntaje:.2f}, Umbral: {umbral:.2f}, Peso total: {peso_total:.2f}, "
        f"Persistencia: {persistencia:.2f}, Estrategias activas: {estrategias_activas}"
    )
    razones: list[str] = []

    ok_pers, valor_pers, minimo_pers = trader._evaluar_persistencia(
        symbol, estado, df, pesos_symbol, tendencia_actual, puntaje, umbral, estrategias
    )
    if not ok_pers:
        razones.append("persistencia")

    if razones:
        agresivo = config_actual.get("modo_agresivo", False)
        if not agresivo or len(razones) > 2:
            log.info(f"❌ [{symbol}] Rechazo acumulado por: {razones}")
            return None

    rsi = calcular_rsi(df)
    momentum = calcular_momentum(df)
    slope = calcular_slope(df)
    precio_actual = float(df["close"].iloc[-1])
    try:
        cantidad_simulada = await asyncio.wait_for(
            trader._calcular_cantidad_async(symbol, precio_actual), timeout=10
        )
    except Exception as e:  # noqa: BLE001
        log.exception(f"[{symbol}] Error calculando cantidad: {e}")
        raise
    log.debug(f"[{symbol}] Cantidad simulada calculada: {cantidad_simulada}")

    score_tecnico = 1.0
    if trader.usar_score_tecnico:
        score_tecnico, puntos = trader._calcular_score_tecnico(
            symbol,
            df,
            rsi,
            momentum,
            tendencia_actual,
            direccion,
        )
        log.debug(f"[{symbol}] Score técnico: {score_tecnico:.2f}, Componentes: {puntos}")

    permitido, motivo = trader.engine._verificar_checks(
        symbol,
        estrategias_persistentes,
        puntaje,
        score_tecnico,
        umbral=umbral,
        umbral_score=config_actual.get("umbral_score_tecnico", 1.0),
        val_score=evaluacion.get("score_validaciones", 1.0),
        umbral_validacion=config_actual.get("umbral_validacion", 0.5),
        diversidad_minima=config_actual.get("diversidad_minima", 1),
        contradiccion=False,
    )
    if not permitido:
        log.info(f"❌ [{symbol}] Filtro técnico final bloqueó la entrada: {motivo}")
        return None

    log.info(f"✅ [{symbol}] Señal de entrada generada con {len(estrategias_activas)} estrategias.")
    precio = precio_actual
    sl, tp = calcular_tp_sl_adaptativos(
        symbol,
        df,
        config_actual,
        trader.capital_por_simbolo.get(symbol, 0),
        precio,
    )

    if not distancia_minima_valida(precio, sl, tp):
        log.warning(
            f"📏 [{symbol}] Distancia SL/TP insuficiente. SL: {sl:.2f} TP: {tp:.2f}"
        )
        return None

    ratio_min = config_actual.get("ratio_minimo_beneficio", 1.5)
    if not await asyncio.to_thread(distancia_minima_valida, precio, sl, tp):
        log.warning(
            f"🚫 [{symbol}] Ratio beneficio/riesgo < {ratio_min:.2f}"
        )
        return None

    evaluacion = evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = evaluacion["score_total"]
    vol = 0.0
    if "volume" in df.columns and len(df) > 20:
        vol = df["volume"].iloc[-1] / (df["volume"].rolling(20).mean().iloc[-1] or 1)
    volatilidad = volatilidad_actual
    pesos_simbolo = cargar_pesos_tecnicos(symbol)
    score_max = sum(pesos_simbolo.values())
    umbral_tecnico = calc_umbral_tecnico(
        score_max,
        tendencia_actual,
        volatilidad,
        vol,
        estrategias_persistentes,
    )
    log.info(
        f"- Umbral adaptativo: {umbral_tecnico:.2f} → {'✅' if score_total >= umbral_tecnico else '❌'} Entrada permitida"
    )
    if score_total < umbral_tecnico:
        log.info(f"[{symbol}] Entrada rechazada por score técnico {score_total:.2f} < {umbral_tecnico:.2f}")
        return None

    resultado = {
        "symbol": symbol,
        "precio": precio,
        "sl": sl,
        "tp": tp,
        "estrategias": estrategias_activas,
        "puntaje": puntaje,
        "umbral": umbral,
        "tendencia": tendencia_actual,
        "direccion": direccion,
        "score_tecnico": score_tecnico if trader.usar_score_tecnico else None,
        "detalles_tecnicos": evaluacion.get("detalles", {}),
        "volatilidad": volatilidad,
    }
    log.debug(f"✅ [{symbol}] Evaluación de entrada completada")
    return resultado
