import pandas as pd
from indicadores.estocastico import calcular_cruce_estocastico


def estrategia_estocastico(df: pd.DataFrame) ->dict:
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    cruce = calcular_cruce_estocastico(df)
    if cruce:
        return {'activo': True, 'mensaje': 'Cruce estocástico alcista'}
    return {'activo': False, 'mensaje': 'Sin cruce estocástico'}
