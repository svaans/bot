import pandas as pd


def detectar_ruptura_por_rango(df: pd.DataFrame, umbral_std: float=1.5) ->bool:
    if 'high' not in df or 'low' not in df or len(df) < 20:
        return False
    df = df.copy()
    df['rango'] = df['high'] - df['low']
    media_rango = df['rango'].iloc[:-1].mean()
    std_rango = df['rango'].iloc[:-1].std()
    rango_actual = df['rango'].iloc[-1]
    return rango_actual > media_rango + umbral_std * std_rango
