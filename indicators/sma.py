import pandas as pd


def calcular_cruce_sma(data, rapida: int = 20, lenta: int = 50) -> bool:
    """Determina si existe cruce alcista de medias móviles.

    Acepta un ``DataFrame`` con la columna ``close`` o directamente una ``Series``
    de precios de cierre.
    """
    serie = data['close'] if isinstance(data, pd.DataFrame) else data
    if serie is None or len(serie) < lenta:
        return False
    sma_rapida = serie.rolling(window=rapida).mean()
    sma_lenta = serie.rolling(window=lenta).mean()
    return sma_rapida.iloc[-1] > sma_lenta.iloc[-1]
