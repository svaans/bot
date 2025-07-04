import pandas as pd


def descending_scallop(df: pd.DataFrame) ->dict:
    """
    Detecta el patrón bajista Descending Scallop:
    - Comienza con retroceso alcista
    - Luego una caída prolongada con pendiente negativa
    """
    if len(df) < 20:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    precios = df['close'].tail(20)
    inicio = precios.iloc[0]
    final = precios.iloc[-1]
    maximo = precios.max()
    if inicio == maximo and final < inicio:
        return {'activo': True, 'mensaje': 'Descending Scallop detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón Descending Scallop'}
