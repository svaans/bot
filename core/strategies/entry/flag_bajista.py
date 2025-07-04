import pandas as pd


def flag_bajista(df: pd.DataFrame) ->dict:
    """
    Detecta patrón de continuación bajista tipo bandera ("flag").
    Requiere que las últimas velas muestren un canal descendente tras caída.
    """
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    velas = df.tail(5)
    diffs = velas['close'].diff().dropna()
    if all(diffs < 0):
        return {'activo': True, 'mensaje': 'Patrón banderín bajista detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón flag bajista'}
