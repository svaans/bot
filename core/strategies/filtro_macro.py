"""Filtro macro de mercado: tendencia de BTC + Fear & Greed Index.

BTC macro filter: cuando BTC cae bajo su EMA200 diaria, los rebotes en
alts tienen esperanza negativa. El estudio empírico (backtest_rapido.py
--study3, 5 años, 70/30) muestra que sin este filtro las configs con TP
amplio pasan de PF>1.4 a PF<0.9 fuera de muestra.

Fear & Greed filter: bloquea entradas cuando el índice supera el umbral
de codicia extrema (default >75). Fuente: alternative.me/fng (sin API key).
El índice se refresca máximo una vez al día y se cachea en memoria.

Ambos filtros se desactivan por defecto; se activan en ProductionConfig o
vía variables de entorno FILTRO_MACRO_BTC_ENABLED=true /
FILTRO_FEAR_GREED_ENABLED=true.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request

import pandas as pd

_logger = logging.getLogger(__name__)

# Cache en memoria: (timestamp_ultimo_fetch, valor_actual)
_fg_cache: tuple[float, int | None] = (0.0, None)
_FG_TTL = 3_600 * 6  # refrescar cada 6 horas máximo


def obtener_fear_greed() -> int | None:
    """Obtiene el valor actual del Fear & Greed Index (0-100) de alternative.me.

    Cachea el resultado en memoria durante _FG_TTL segundos para no hacer
    una petición HTTP en cada vela. Devuelve None si no hay conectividad.
    """
    global _fg_cache
    ts_ahora, valor_cache = _fg_cache
    if valor_cache is not None and (time.time() - ts_ahora) < _FG_TTL:
        return valor_cache

    try:
        url = "https://api.alternative.me/fng/?limit=1&format=json"
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.load(r)
        valor = int(data["data"][0]["value"])
        _fg_cache = (time.time(), valor)
        _logger.info("Fear&Greed actualizado: %d (%s)",
                     valor, data["data"][0].get("value_classification", ""))
        return valor
    except Exception as exc:
        _logger.warning("Fear&Greed no disponible: %s", exc)
        # Si falla, no bloqueamos entradas — devolvemos el valor cacheado si lo hay
        return valor_cache


def fear_greed_permite_entrada(
    umbral_codicia: int = 75,
    umbral_miedo: int = 0,
) -> bool | None:
    """Retorna False si el índice está fuera de la zona de operación.

    Con ``umbral_miedo > 0`` implementa zona_neutral: bloquea tanto la
    codicia extrema (F&G > umbral_codicia) como el pánico extremo
    (F&G < umbral_miedo). El estudio empírico muestra que en pánico
    extremo el downtrend sigue activo y las entradas tienen peor RR.

    Retorna None si no hay datos (el llamador no debe bloquear).
    """
    valor = obtener_fear_greed()
    if valor is None:
        return None
    if valor > umbral_codicia:
        return False
    if umbral_miedo > 0 and valor < umbral_miedo:
        return False
    return True


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
