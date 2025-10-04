import pandas as pd
from indicadores.medias import calcular_cruce_medias


def estrategia_cruce_medias(df: pd.DataFrame) ->dict:
    if len(df) < 30:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}
    cruce = calcular_cruce_medias(df)
    if cruce:
        return {'activo': True, 'mensaje': 'Cruce alcista de medias detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce de medias'}
