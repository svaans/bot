import pandas as pd

def calcular_cruce_ema_stochrsi(
    df: pd.DataFrame,
    ema_fast: int = 9,
    ema_slow: int = 21,
    rsi_period: int = 14,
    stoch_period: int = 14,
    d_period: int = 3,
) -> bool:
    if "close" not in df or len(df) < max(
        ema_slow,
        rsi_period + stoch_period + d_period,
    ):
        return False

    df = df.copy()

    # Cruce EMA
    ema_rapida = df["close"].ewm(span=ema_fast, adjust=False).mean()
    ema_lenta = df["close"].ewm(span=ema_slow, adjust=False).mean()
    cruce_ema = (
        ema_rapida.iloc[-2] < ema_lenta.iloc[-2]
        and ema_rapida.iloc[-1] > ema_lenta.iloc[-1]
    )

    # RSI
    delta = df["close"].diff()
    ganancia = delta.clip(lower=0)
    perdida = -delta.clip(upper=0)
    media_ganancia = ganancia.ewm(
        alpha=1 / rsi_period, adjust=False, min_periods=rsi_period
    ).mean()
    media_perdida = perdida.ewm(
        alpha=1 / rsi_period, adjust=False, min_periods=rsi_period
    ).mean()
    rs = media_ganancia / media_perdida
    rsi = 100 - 100 / (1 + rs)

    rsi_recent = rsi.dropna().tail(stoch_period + d_period)
    if len(rsi_recent) < stoch_period + d_period:
        return False

    lowest = rsi_recent.rolling(window=stoch_period).min()
    highest = rsi_recent.rolling(window=stoch_period).max()
    stoch_k = 100 * (rsi_recent - lowest) / (highest - lowest)
    stoch_d = stoch_k.rolling(window=d_period).mean()

    cruce_stochrsi = (
        stoch_k.iloc[-2] < stoch_d.iloc[-2]
        and stoch_k.iloc[-1] > stoch_d.iloc[-1]
    )
    zona_baja = stoch_k.iloc[-1] < 20

    return cruce_ema and cruce_stochrsi and zona_baja
