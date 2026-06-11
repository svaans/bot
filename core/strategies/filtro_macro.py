"""Filtro macro de mercado: tendencia de BTC como interruptor de entradas.

Las altcoins están altamente correlacionadas con BTC: cuando BTC cae bajo
su EMA200 diaria, los rebotes en alts tienen esperanza negativa. El estudio
empírico (``backtesting/backtest_rapido.py --study3``, 5 años, validación
70/30) muestra que sin este filtro las configuraciones con TP amplio pasan
de PF>1.4 a PF<0.9 fuera de muestra, y con él todas las variantes ganadoras
se mantienen rentables en test.

Se desactiva por defecto (``filtro_macro_btc_enabled=False``); se activa en
``ProductionConfig`` o con la variable de entorno
``FILTRO_MACRO_BTC_ENABLED=true``.
"""
from __future__ import annotations

import pandas as pd


def btc_en_tendencia(df_btc: pd.DataFrame | None, periodo: int = 200) -> bool | None:
    """``True``/``False`` si BTC cotiza sobre/bajo su EMA del ``periodo``.

    Devuelve ``None`` (sin veredicto) si no hay datos suficientes; el
    llamador no debe bloquear entradas en ese caso.
    """
    if df_btc is None or "close" not in getattr(df_btc, "columns", []):
        return None
    if len(df_btc) < periodo:
        return None
    close = df_btc["close"].astype(float)
    ema = close.ewm(span=periodo, adjust=False).mean().iloc[-1]
    actual = float(close.iloc[-1])
    if pd.isna(ema) or actual <= 0:
        return None
    return actual > float(ema)
