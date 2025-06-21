# estrategias_salida/salida_stoploss.py

import pandas as pd

from core.tendencia import detectar_tendencia
from core.estrategias import (
    obtener_estrategias_por_tendencia,
    ESTRATEGIAS_POR_TENDENCIA,
)
from core.utils import validar_dataframe
from core.adaptador_dinamico import calcular_umbral_adaptativo
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.pesos import gestor_pesos
from core.logger import configurar_logger
from core.salida_utils import resultado_salida
from indicadores.rsi import calcular_rsi
from indicadores.slope import calcular_slope
from indicadores.vwap import calcular_vwap

log = configurar_logger("salida_stoploss")

pesos = gestor_pesos.pesos

def validar_sl_tecnico(df: pd.DataFrame, direccion: str = "long") -> bool:
    """Comprueba si existen razones técnicas sólidas para ejecutar el SL."""
    try:
        if not validar_dataframe(df, ["close"]):
            return True

        rsi = calcular_rsi(df)
        slope = calcular_slope(df.tail(5))
        precio = df["close"].iloc[-1]
        ma9 = df["close"].rolling(window=9).mean().iloc[-1]
        ma20 = df["close"].rolling(window=20).mean().iloc[-1]
        vwap = calcular_vwap(df)

        debajo_ma = precio < ma9 and precio < ma20
        debajo_vwap = vwap is not None and precio < vwap
        velas_rojas = (df["close"].diff().tail(5) < 0).sum()
        persistencia = velas_rojas >= 3

        if direccion in ["long", "compra"]:
            return (
                (rsi is not None and rsi < 40)
                and slope < 0
                and (debajo_vwap or debajo_ma)
                and persistencia
            )
        return True

    except Exception as e:
        log.warning(f"Error validando SL técnico: {e}")
        return True
    
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
        min_estrategias_relevantes = config.get("min_estrategias_relevantes_sl", 3) if config else 3

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
            len(activas_relevantes) >= min_estrategias_relevantes and
            puntaje >= factor_umbral * umbral
        )

        if condiciones_validas:
            mensaje = (
                f"🛡️ SL evitado en {symbol} → Tendencia: {tendencia}, "
                f"Estrategias activas: {activas}, Puntaje: {puntaje:.2f}/{umbral:.2f}"
            )
            log.info(mensaje)
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
        if (
            direccion in ("long", "compra") and precio_actual <= precio_entrada
        ) or (
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
    estrategias_activas = evaluacion.get("estrategias_activas", {}) if evaluacion else {}
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

    factor_umbral = config.get("factor_umbral_sl", 0.7) if config else 0.7
    min_estrategias_relevantes = config.get("min_estrategias_relevantes_sl", 3) if config else 3
    esperadas = ESTRATEGIAS_POR_TENDENCIA.get(tendencia, [])
    activas_relevantes = [e for e in activas if e in esperadas]
    condiciones_validas = (
        len(activas_relevantes) >= min_estrategias_relevantes
        and puntaje >= factor_umbral * umbral
    )

    duracion = orden.get("duracion_en_velas", 0)
    max_velas = config.get("max_velas_sin_tp", 10) if config else 10
    intentos = len(orden.get("sl_evitar_info", []))
    max_evitar = config.get("max_evitar_sl", 2) if config else 2

    cerrar_forzado = validar_sl_tecnico(df, direccion) or puntaje < 0.75 * umbral or duracion >= max_velas or intentos >= max_evitar

    if condiciones_validas and not cerrar_forzado:
        log.info(
            f"🛡️ SL evitado en {symbol} | Puntaje: {puntaje:.2f}/{umbral:.2f} | Velas abiertas: {duracion}"
        )
        return resultado_salida(
            "Stop Loss",
            False,
            "SL tocado pero indicadores válidos para mantener",
            motivo="SL tocado pero indicadores válidos para mantener",
            evitado=True,
        )
    
    if puntaje >= 2.5 * umbral:
        log.info(
            f"🛡️ SL evitado por score excepcional en {symbol} → {puntaje:.2f}/{umbral:.2f}"
        )
        return resultado_salida(
            "Stop Loss",
            False,
            "Score técnico muy alto",
            motivo="Score técnico muy alto",
            evitado=True,
        )

    log.info(
        f"🔴 SL forzado en {symbol} | Score técnico: {puntaje:.2f}/{umbral:.2f} | Velas abiertas: {duracion}"
    )
    return resultado_salida(
        "Stop Loss",
        True,
        f"SL forzado | Score: {puntaje:.2f}/{umbral:.2f} | Velas: {duracion}",
        motivo=f"SL forzado | Score: {puntaje:.2f}/{umbral:.2f} | Velas: {duracion}",
        evitado=False,
        logger=log,
    )
    

    

