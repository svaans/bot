import pandas as pd
from .validaciones_comunes import rsi_cruce_descendente


def estrategia_rsi_invertida(df: pd.DataFrame) ->dict:
    """Estrategia RSI invertida.

    Se asegura de que el DataFrame de entrada sea válido antes de
    procesarlo para evitar errores cuando las velas no estén
    inicializadas o sean insuficientes.
    """

    if df is None or len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    valido, rsi = rsi_cruce_descendente(df, umbral=70)
    if rsi is None:
        return {'activo': False, 'mensaje': 'RSI no disponible'}
    if valido:
        return {'activo': True, 'mensaje':
            'RSI cruzando hacia abajo desde sobrecompra'}
    return {'activo': False, 'mensaje': 'Sin cruce descendente de RSI'}
