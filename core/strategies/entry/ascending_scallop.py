import pandas as pd


def ascending_scallop(df: pd.DataFrame) ->dict:
    """
    Detecta patrón Ascending Scallop:
    - Inicio en un punto bajo, recuperación en forma curva.
    """
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    precios = df['close'].tail(20)
    inicio = precios.iloc[0]
    final = precios.iloc[-1]
    minimo = precios.min()
    if inicio > minimo and final > inicio:
        return {'activo': True, 'mensaje': 'Ascending Scallop detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón Ascending Scallop'}
