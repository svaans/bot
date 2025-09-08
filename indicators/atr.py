import pandas as pd


def calcular_atr(df: pd.DataFrame, periodo: int = 14) -> float | None:
    """Calcula el Average True Range (ATR) usando el método de Wilder.

    Limpia los atributos de las series intermedias para evitar que pandas
    intente compararlos cuando se concatena con el caché de indicadores.
    """
    columnas = {"high", "low", "close"}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)

    def _safe_series(s: pd.Series, name: str) -> pd.Series:
        s = s.astype(float).copy(deep=False)
        try:
            s.attrs.clear()
        except Exception:
            pass
        return s.rename(name)

    tr1 = _safe_series(high - low, "tr1")
    tr2 = _safe_series((high - prev_close).abs(), "tr2")
    tr3 = _safe_series((low - prev_close).abs(), "tr3")

    tr = (
        pd.concat([tr1, tr2, tr3], axis=1, copy=False)
        .max(axis=1)
        .fillna(0.0)
    )
    atr = tr.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    return float(atr.iloc[-1]) if not atr.empty else None
