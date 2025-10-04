import pandas as pd
from indicadores.rango import detectar_ruptura_por_rango


def estrategia_rango(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    ruptura = detectar_ruptura_por_rango(df)
    if ruptura:
        return {'activo': True, 'mensaje': 'Ruptura de rango detectada'}
    return {'activo': False, 'mensaje': 'Sin ruptura de rango'}
