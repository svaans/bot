import pandas as pd


def diamond_bottom(df: pd.DataFrame) ->dict:
    """
    Detecta un patrón Diamond Bottom:
    - Rango inicial estrecho → expansión en el medio → contracción final
    """
    if len(df) < 40:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    rango_inicial = df['high'].iloc[:10].max() - df['low'].iloc[:10].min()
    rango_medio = df['high'].iloc[10:30].max() - df['low'].iloc[10:30].min()
    rango_final = df['high'].iloc[30:].max() - df['low'].iloc[30:].min()
    if rango_inicial < rango_medio and rango_final < rango_medio:
        return {'activo': True, 'mensaje': 'Diamond Bottom detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón Diamond Bottom'}
