import pandas as pd
from core.utils import validar_dataframe
from core.tendencia import detectar_tendencia

def salida_por_tendencia(orden, df):
    """
    Cierra si la tendencia cambia respecto a la entrada.
    """
    tendencia_entrada = orden.get("tendencia")
    if not tendencia_entrada:
        return {"cerrar": False, "razon": "Sin tendencia previa registrada"}

    try:
        tendencia_actual = detectar_tendencia(orden["symbol"], df)
        if tendencia_actual != tendencia_entrada:
            return {
                "cerrar": True,
                "razon": f"Cambio de tendencia: {tendencia_entrada} → {tendencia_actual}"
            }
        else:
            return {"cerrar": False, "razon": "Tendencia estable"}
    except Exception as e:
        return {"cerrar": False, "razon": f"Error evaluando tendencia: {e}"}
    

def verificar_reversion_tendencia(symbol, df, tendencia_anterior):
    if not validar_dataframe(df, ["high", "low", "close"]):
        return False

    nueva_tendencia = detectar_tendencia(symbol, df)
    return nueva_tendencia != tendencia_anterior


