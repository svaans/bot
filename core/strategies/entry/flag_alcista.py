import pandas as pd


def flag_alcista(df: pd.DataFrame) ->dict:
    """
    Detecta un patrón de continuación alcista tipo bandera ("flag").
    Requiere que las últimas velas muestren un canal ascendente después de un impulso.
    """
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    velas = df.tail(5)
    diffs = velas['close'].diff().dropna()
    if all(diffs > 0):
        return {'activo': True, 'mensaje': 'Posible banderín alcista (flag)'}
    return {'activo': False, 'mensaje': 'Sin patrón flag alcista'}
