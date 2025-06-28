import pandas as pd
from core.utils.validacion import validar_dataframe
from core.strategies.tendencia import detectar_tendencia
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida

log = configurar_logger("salida_por_tendencia")

def salida_por_tendencia(orden, df):
    """
    Cierra si la tendencia cambia respecto a la entrada.
    """
    tendencia_entrada = orden.get("tendencia")
    if not tendencia_entrada:
        return resultado_salida(
            "Tecnico",
            False,
            "Sin tendencia previa registrada",
        )

    try:
        tendencia_actual, _ = detectar_tendencia(orden["symbol"], df)
        if tendencia_actual != tendencia_entrada:
            return resultado_salida(
                "Tecnico",
                True,
                f"Cambio de tendencia: {tendencia_entrada} → {tendencia_actual}",
                logger=log,
            )
        else:
            return resultado_salida("Tecnico", False, "Tendencia estable")
    except Exception as e:
        return resultado_salida("Tecnico", False, f"Error evaluando tendencia: {e}")
    

def confirmar_reversion_multi(df: pd.DataFrame, tendencia_anterior: str, velas: int = 3) -> bool:
    """Confirma una reversión validando máximos o mínimos consecutivos."""

    if not validar_dataframe(df, ["high", "low"]) or len(df) < velas + 1:
        return False

    ventana = df.tail(velas + 1)

    if tendencia_anterior == "bajista":
        highs = ventana["high"].reset_index(drop=True)
        return all(highs[i] > highs[i - 1] for i in range(1, len(highs)))

    if tendencia_anterior == "alcista":
        lows = ventana["low"].reset_index(drop=True)
        return all(lows[i] < lows[i - 1] for i in range(1, len(lows)))

    return False


def verificar_reversion_tendencia(symbol, df, tendencia_anterior, velas: int = 1):
    if not validar_dataframe(df, ["high", "low", "close"]):
        return False

    nueva_tendencia, _ = detectar_tendencia(symbol, df)
    if nueva_tendencia == tendencia_anterior:
        return False

    if velas > 1:
        return confirmar_reversion_multi(df, tendencia_anterior, velas)

    return True


