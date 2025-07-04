import pandas as pd


def pennant_bajista(df: pd.DataFrame) ->dict:
    """
    Detecta patrón Pennant bajista:
    - Consolidación estrecha
    - Ruptura por debajo de la media
    """
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    df = df.tail(15)
    variacion = df['close'].pct_change().abs().mean()
    ruptura_bajista = df['close'].iloc[-1] < df['close'].mean()
    if variacion < 0.01 and ruptura_bajista:
        return {'activo': True, 'mensaje': 'Pennant bajista detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón Pennant bajista'}
