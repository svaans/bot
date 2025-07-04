import pandas as pd


def pennant(df: pd.DataFrame) ->dict:
    """
    Detecta patrón Pennant:
    - Consolidación de baja volatilidad
    - Ruptura al alza (precio actual > media)
    """
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    df = df.tail(15)
    variacion_media = df['close'].pct_change().abs().mean()
    ruptura = df['close'].iloc[-1] > df['close'].mean()
    if variacion_media < 0.01 and ruptura:
        return {'activo': True, 'mensaje': 'Pennant alcista detectado'}
    return {'activo': False, 'mensaje': 'Sin patrón Pennant'}
