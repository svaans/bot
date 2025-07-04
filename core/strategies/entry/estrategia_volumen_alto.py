import pandas as pd
from .validaciones_comunes import volumen_suficiente


def estrategia_volumen_alto(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    if volumen_suficiente(df):
        return {'activo': True, 'mensaje':
            'Volumen inusualmente alto detectado'}
    return {'activo': False, 'mensaje': 'Volumen dentro de rango normal'}
