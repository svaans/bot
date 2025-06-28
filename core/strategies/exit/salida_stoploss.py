# estrategias_salida/salida_stoploss.py

import pandas as pd

from core.strategies.tendencia import detectar_tendencia
from core.estrategias import ESTRATEGIAS_POR_TENDENCIA
from core.utils.validacion import validar_dataframe
from core.adaptador_dinamico import calcular_umbral_adaptativo
from core.adaptador_umbral import calcular_umbral_salida_adaptativo
from core.strategies.entry.gestor_entradas import evaluar_estrategias
from core.strategies.pesos import gestor_pesos
from core.utils import configurar_logger, build_log_message
from core.strategies.exit.salida_utils import resultado_salida
from indicators.rsi import calcular_rsi
from indicators.slope import calcular_slope
from indicators.vwap import calcular_vwap
from indicators.momentum import calcular_momentum
from core.scoring import calcular_score_tecnico

log = configurar_logger("salida_stoploss")

pesos = gestor_pesos.pesos

def validar_sl_tecnico(
    df: pd.DataFrame, direccion: str = "long", symbol: str | None = None
) -> float:
    """Devuelve un puntaje de 0 a 1 sobre la fortaleza de cerrar por SL.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con los datos de precios.
    direccion : str, optional
        Dirección de la operación, "long" por defecto.
    symbol : str | None, optional
        Símbolo del activo para los logs y el cálculo de score.
    """
    try:
        if not validar_dataframe(df, ["close"]):
            return 1.0

        rsi = calcular_rsi(df)
        slope = calcular_slope(df.tail(5))
        momentum = calcular_momentum(df)
        precio = df["close"].iloc[-1]
        ma9 = df["close"].rolling(window=9).mean().iloc[-1]
        ma20 = df["close"].rolling(window=20).mean().iloc[-1]
        vwap = calcular_vwap(df)

        debajo_ma = precio < ma9 and precio < ma20
        debajo_vwap = vwap is not None and precio < vwap
        velas_rojas = (df["close"].diff().tail(5) < 0).sum()
        persistencia = velas_rojas >= 3

        score = calcular_score_tecnico(
            df,
            rsi,
            momentum,
            slope,
            "bajista" if direccion in ["long", "compra"] else "alcista",
            symbol=symbol,
        )

        if direccion in ["long", "compra"]:
            condiciones = score / 4
            if debajo_vwap or debajo_ma:
                condiciones += 0.25
            if persistencia:
                condiciones += 0.25
            return max(0.0, min(1.0, condiciones))
        return 1.0

    except Exception as e:
        log.warning(f"Error validando SL técnico: {e}")
        return 1.0
    
def salida_stoploss(orden: dict, df: pd.DataFrame, config: dict = None) -> dict:
    """
    Evalúa si debe cerrarse una orden cuyo precio ha tocado el SL,
    o si puede mantenerse por razones técnicas justificadas.
    """
    try:
        symbol = orden.get("symbol")
        if not symbol or not validar_dataframe(df, ["high", "low", "close"]):
            return resultado_salida(
                "Stop Loss",
                True,
                "Datos inválidos o símbolo no definido",
                logger=log,
            )

        sl = orden.get("stop_loss")
        precio_actual = df["close"].iloc[-1]

        # 🛑 Si el precio no ha tocado el SL, no se evalúa nada
        if precio_actual > sl:
            return resultado_salida(
                "Stop Loss",
                False,
                f"SL no alcanzado aún (precio: {precio_actual:.2f} > SL: {sl:.2f})",
            )

        # ⚙️ Evaluación técnica solo si se ha tocado el SL
        tendencia, _ = detectar_tendencia(symbol, df)
        if not tendencia:
            return resultado_salida(
                "Stop Loss",
                True,
                "Tendencia no identificada",
                logger=log,
            )

        evaluacion = evaluar_estrategias(symbol, df, tendencia)
        if not evaluacion:
            return resultado_salida(
                "Stop Loss",
                True,
                "Evaluación de estrategias fallida",
                logger=log,
            )

        estrategias_activas = evaluacion.get("estrategias_activas", {})
        puntaje = evaluacion.get("puntaje_total", 0)
        activas = [k for k, v in estrategias_activas.items() if v]

        # Configuración personalizada
        factor_umbral = config.get("factor_umbral_sl", 0.7) if config else 0.7
        min_estrategias_relevantes = (
            config.get("min_estrategias_relevantes_sl", 3) if config else 3
        )

        # Carga de pesos para umbral
        pesos_symbol = pesos.get(symbol, {})
        umbral = calcular_umbral_adaptativo(
            symbol,
            df,
            estrategias_activas,
            pesos_symbol,
            persistencia=0.0,
            config=config,
        )

        # Concordancia con la tendencia actual
        esperadas = ESTRATEGIAS_POR_TENDENCIA.get(tendencia, [])
        activas_relevantes = [e for e in activas if e in esperadas]

        condiciones_validas = (
            len(activas_relevantes) >= min_estrategias_relevantes
            and puntaje >= factor_umbral * umbral
        )

        if condiciones_validas:
            log.info(
                build_log_message(
                    "sl_evitar",
                    symbol=symbol,
                    puntaje=round(puntaje, 2),
                    umbral=round(umbral, 2),
                    tendencia=tendencia,
                    estrategias=activas,
                )
            )
            return resultado_salida(
                "Stop Loss",
                False,
                "SL evitado por validación técnica y concordancia con tendencia",
            )

        return resultado_salida(
            "Stop Loss",
            True,
            "Condiciones técnicas débiles para mantener",
            logger=log,
        )

    except Exception as e:
        return resultado_salida(
            "Stop Loss",
            True,
            f"Error interno en SL: {e}",
            logger=log,
        )
    
def verificar_salida_stoploss(
    orden: dict, df: pd.DataFrame, config: dict | None = None
) -> dict:
    """Determina si debe ejecutarse el Stop Loss o mantenerse la operación."""

    if df is None or not isinstance(df, pd.DataFrame):
        return resultado_salida(
            "Stop Loss",
            False,
            "❌ DataFrame no válido (None o tipo incorrecto)",
            motivo="❌ DataFrame no válido (None o tipo incorrecto)",
            evitado=False,
        )
    if df.empty or len(df) < 15:
        return resultado_salida(
            "Stop Loss",
            False,
            "❌ DataFrame insuficiente para evaluar SL",
            motivo="❌ DataFrame insuficiente para evaluar SL",
            evitado=False,
        )
    if not validar_dataframe(df, ["close", "high", "low"]):
        return resultado_salida(
            "Stop Loss",
            False,
            "Datos insuficientes",
            motivo="Datos insuficientes",
            evitado=False,
        )
    if not all(k in orden for k in ["precio_entrada", "stop_loss", "direccion"]):
        return resultado_salida(
            "Stop Loss",
            False,
            "❌ Orden incompleta",
            motivo="❌ Orden incompleta",
            evitado=False,
        )
    
    symbol = orden.get("symbol", "SYM")
    
    precio_actual = float(df["close"].iloc[-1])
    precio_entrada = orden.get("precio_entrada", precio_actual)
    direccion = orden.get("direccion", "long")
    precio_actual = float(df["close"].iloc[-1])
    precio_entrada = orden.get("precio_entrada", precio_actual)
    sl_config = orden.get("stop_loss")

    # --- Cierre inmediato por Break Even ---
    if orden.get("break_even_activado"):
        if (direccion in ("long", "compra") and precio_actual <= precio_entrada) or (
            direccion in ("short", "venta") and precio_actual >= precio_entrada
        ):
            log.info(
                f"🟢 Cierre por Break Even en {symbol} | Precio actual: {precio_actual:.2f} <= Entrada: {precio_entrada:.2f}"
            )
            return {"cerrar": True, "motivo": "Break Even", "evitado": False}

    # --- Cálculo dinámico del SL ---
    atr = None
    if df is not None and len(df) >= 20:
        atr = (df["high"].tail(20) - df["low"].tail(20)).mean()

    ratio = config.get("sl_ratio", 1.5) if config else 1.5
    if atr is not None:
        sl_dinamico = (
            precio_entrada - atr * ratio
            if direccion in ("long", "compra")
            else precio_entrada + atr * ratio
        )
        if direccion in ("long", "compra"):
            sl_config = max(sl_config, sl_dinamico)
        else:
            sl_config = min(sl_config, sl_dinamico)

    orden["stop_loss"] = sl_config

    if (direccion in ("long", "compra") and precio_actual > sl_config) or (
        direccion in ("short", "venta") and precio_actual < sl_config
    ):
        return resultado_salida(
            "Stop Loss",
            False,
            f"SL no alcanzado aún (precio: {precio_actual:.2f} vs SL: {sl_config:.2f})",
            motivo=f"SL no alcanzado aún (precio: {precio_actual:.2f} vs SL: {sl_config:.2f})",
            evitado=False,
        )

    tendencia, _ = detectar_tendencia(symbol, df)
    evaluacion = evaluar_estrategias(symbol, df, tendencia)
    estrategias_activas = (
        evaluacion.get("estrategias_activas", {}) if evaluacion else {}
    )
    puntaje = evaluacion.get("puntaje_total", 0) if evaluacion else 0
    activas = [k for k, v in estrategias_activas.items() if v]

    pesos_symbol = pesos.get(symbol, {})
    umbral = calcular_umbral_adaptativo(
        symbol,
        df,
        estrategias_activas,
        pesos_symbol,
        persistencia=0.0,
        config=config,
    )

    rsi = calcular_rsi(df)
    momentum = calcular_momentum(df)
    slope = calcular_slope(df)
    score_tecnico = calcular_score_tecnico(
        df,
        rsi,
        momentum,
        slope,
        "bajista" if direccion in ["long", "compra"] else "alcista",
        symbol=symbol,
    )

    umbral_salida = calcular_umbral_salida_adaptativo(
        symbol,
        df,
        config or {},
        {
            "estrategias_activas": estrategias_activas,
            "pesos_symbol": pesos_symbol,
        },
    )

    factor_umbral = config.get("factor_umbral_sl", 0.7) if config else 0.7
    min_estrategias_relevantes = (
        config.get("min_estrategias_relevantes_sl", 3) if config else 3
    )
    esperadas = ESTRATEGIAS_POR_TENDENCIA.get(tendencia, [])
    activas_relevantes = [e for e in activas if e in esperadas]
    condiciones_validas = (
        len(activas_relevantes) >= min_estrategias_relevantes
        and puntaje >= factor_umbral * umbral
        and score_tecnico >= umbral_salida
    )

    fallos = []
    if len(activas_relevantes) < min_estrategias_relevantes:
        fallos.append("estrategias_relevantes")
    if puntaje < factor_umbral * umbral:
        fallos.append("puntaje")
    if score_tecnico < umbral_salida:
        fallos.append("score_tecnico")

    duracion = orden.get("duracion_en_velas", 0)
    max_velas = config.get("max_velas_sin_tp", 10) if config else 10
    intentos = len(orden.get("sl_evitar_info") or [])
    max_evitar = config.get("max_evitar_sl", 2) if config else 2

    sl_conf = validar_sl_tecnico(df, direccion, symbol)
    cerrar_forzado = (
        sl_conf >= 0.5
        or puntaje < 0.75 * umbral
        or score_tecnico < 0.5 * umbral_salida
        or duracion >= max_velas
        or intentos >= max_evitar
    )

    if cerrar_forzado:
        fallos.append("cierre_forzado")

    if condiciones_validas and not cerrar_forzado:
        log.info(
            build_log_message(
                "sl_evitar",
                symbol=symbol,
                score=round(score_tecnico, 2),
                umbral=round(umbral_salida, 2),
                duracion=duracion,
            )
        )
        return resultado_salida(
            "Stop Loss",
            False,
            "SL tocado pero indicadores válidos para mantener",
            motivo="SL tocado pero indicadores válidos para mantener",
            evitado=True,
            score=round(score_tecnico, 2),
            umbral=round(umbral_salida, 2),
        )
    
    if score_tecnico >= 2.5 * umbral_salida:
        log.info(
            build_log_message(
                "sl_evitar_extremo",
                symbol=symbol,
                score=round(score_tecnico, 2),
                umbral=round(umbral_salida, 2),
                duracion=duracion,
            )
        )
        return resultado_salida(
            "Stop Loss",
            False,
            "Score técnico muy alto",
            motivo="Score técnico muy alto",
            evitado=True,
            score=round(score_tecnico, 2),
            umbral=round(umbral_salida, 2),
        )

    log.info(
        build_log_message(
            "sl_forzado",
            symbol=symbol,
            score=round(score_tecnico, 2),
            umbral=round(umbral_salida, 2),
            duracion=duracion,
            failed_checks=fallos,
        )
    )
    return resultado_salida(
        "Stop Loss",
        True,
        f"SL forzado | Score: {score_tecnico:.2f}/{umbral_salida:.2f} | Velas: {duracion}",
        motivo=f"SL forzado | Score: {score_tecnico:.2f}/{umbral_salida:.2f} | Velas: {duracion}",
        evitado=False,
        logger=log,
        score=round(score_tecnico, 2),
        umbral=round(umbral_salida, 2),
    )
    

    

