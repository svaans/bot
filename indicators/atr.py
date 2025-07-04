import pandas as pd


def calcular_atr(df: pd.DataFrame, periodo: int=14) ->float:
    """Calcula el Average True Range (ATR) usando el metodo de Wilder."""
    columnas = {'high', 'low', 'close'}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None
    df = df.copy()
    cierre_prev = df['close'].shift()
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - cierre_prev).abs()
    tr3 = (df['low'] - cierre_prev).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    return atr.iloc[-1] if not atr.empty else None
