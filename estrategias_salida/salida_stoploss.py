# estrategias_salida/salida_stoploss.py

import pandas as pd

from core.tendencia import detectar_tendencia
from core.estrategias import (
    obtener_estrategias_por_tendencia,
    ESTRATEGIAS_POR_TENDENCIA,
)
from core.utils import validar_dataframe
from core.adaptador_umbral import calcular_umbral_adaptativo
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.pesos import gestor_pesos
from core.logger import configurar_logger
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
            return {"cerrar": True, "razon": "Datos inválidos o símbolo no definido"}

        sl = orden.get("stop_loss")
        precio_actual = df["close"].iloc[-1]

        # 🛑 Si el precio no ha tocado el SL, no se evalúa nada
        if precio_actual > sl:
            return {"cerrar": False, "razon": f"SL no alcanzado aún (precio: {precio_actual:.2f} > SL: {sl:.2f})"}

        # ⚙️ Evaluación técnica solo si se ha tocado el SL
        tendencia, _ = detectar_tendencia(symbol, df)
        if not tendencia:
            return {"cerrar": True, "razon": "Tendencia no identificada"}

        evaluacion = evaluar_estrategias(symbol, df, tendencia)
        if not evaluacion:
            return {"cerrar": True, "razon": "Evaluación de estrategias fallida"}

        estrategias_activas = evaluacion.get("estrategias_activas", {})
        puntaje = evaluacion.get("puntaje_total", 0)
        activas = [k for k, v in estrategias_activas.items() if v]

        # Configuración personalizada
        factor_umbral = config.get("factor_umbral_sl", 0.7) if config else 0.7
        min_estrategias_relevantes = config.get("min_estrategias_relevantes_sl", 3) if config else 3

        # Carga de pesos para umbral
        pesos_symbol = pesos.get(symbol, {})
        umbral = calcular_umbral_adaptativo(symbol, df, estrategias_activas, pesos_symbol, config=config)

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
            return {"cerrar": False, "razon": "SL evitado por validación técnica y concordancia con tendencia"}

        return {"cerrar": True, "razon": "Condiciones técnicas débiles para mantener"}

    except Exception as e:
        return {"cerrar": True, "razon": f"Error interno en SL: {e}"}
    
    
def verificar_salida_stoploss(
    orden: dict, df: pd.DataFrame, config: dict | None = None
) -> dict:
    """Determina si el SL debe ejecutarse tras validar condiciones técnicas."""

    if not validar_sl_tecnico(df, orden.get("direccion", "long")):
        return {
            "cerrar": False,
            "motivo": "SL tocado pero indicadores válidos para mantener",
            "evitado": True,
        }
    resultado = salida_stoploss(orden, df, config=config)
    cerrar = resultado.get("cerrar", False)
    motivo = resultado.get("razon", "")
    evitado = not cerrar and "evitado" in motivo.lower()
    return {"cerrar": cerrar, "motivo": motivo, "evitado": evitado}
    

    

