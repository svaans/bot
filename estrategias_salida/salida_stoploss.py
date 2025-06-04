# estrategias_salida/salida_stoploss.py

import pandas as pd

from core.tendencia import detectar_tendencia, obtener_estrategias_por_tendencia, ESTRATEGIAS_POR_TENDENCIA
from core.utils import validar_dataframe
from core.adaptador_umbral import calcular_umbral_adaptativo
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.pesos import cargar_pesos_estrategias
from core.logger import configurar_logger

log = configurar_logger("salida_stoploss")
pesos = cargar_pesos_estrategias()

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
        factor_umbral = config.get("factor_umbral_sl", 0.5) if config else 0.5
        min_estrategias_relevantes = config.get("min_estrategias_relevantes_sl", 2) if config else 2

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
    

