"""Filtro de sentimiento de noticias via CryptoPanic RSS.

Descarga los últimos titulares por símbolo y calcula un score de sentimiento
keyword-based (-1 a +1). Bloquea entradas cuando el sentimiento es muy negativo.

Patrón idéntico a filtro_macro.py: urllib.request + cache en memoria, sin
dependencias externas adicionales. No bloqueante: devuelve None si la fuente
no está disponible y las entradas continúan normalmente.

Activar en producción con FILTRO_NOTICIAS_ENABLED=true o en ProductionConfig.
"""
from __future__ import annotations

import logging
import time
import urllib.request
import xml.etree.ElementTree as ET

_logger = logging.getLogger(__name__)

_TTL = 3_600  # refrescar cada hora por símbolo
_TTL_FALLO = 1_800  # backoff tras un fallo: no reintentar (ni spamear logs) durante este tiempo

# {ticker: (timestamp_fetch, score)}
_cache: dict[str, tuple[float, float]] = {}
# {ticker: timestamp_hasta} — durante el backoff no se vuelve a pedir la fuente
_fallo_hasta: dict[str, float] = {}

# Símbolos de trading → ticker CryptoPanic
SYMBOL_TO_TICKER: dict[str, str] = {
    "BTC/USDT": "BTC", "ETH/USDT": "ETH", "SOL/USDT": "SOL",
    "XRP/USDT": "XRP", "AVAX/USDT": "AVAX",
    "BTC/EUR":  "BTC", "ETH/EUR":  "ETH", "SOL/EUR":  "SOL",
    "XRP/EUR":  "XRP", "AVAX/EUR": "AVAX",
}

_RSS_URL = "https://cryptopanic.com/news/{ticker}/rss/"

_POSITIVOS: frozenset[str] = frozenset({
    "surge", "rally", "bullish", "breakout", "adoption", "approved",
    "approval", "upgrade", "ath", "growth", "partnership", "launch",
    "gain", "rise", "jump", "record", "institutional", "soar", "inflows",
    "integration", "milestone", "support", "secure", "recovered",
})
_NEGATIVOS: frozenset[str] = frozenset({
    "crash", "hack", "scam", "ban", "bearish", "dump", "collapse",
    "lawsuit", "fraud", "delist", "suspend", "warning", "exploit",
    "stolen", "fall", "plunge", "drop", "selloff", "sell-off", "concern",
    "fear", "vulnerable", "breach", "seized", "halted", "probe",
})

_MAX_ITEMS = 15  # número máximo de titulares a analizar


def _puntuar_titulares(titulos: list[str]) -> float:
    """Calcula score [-1, 1] desde lista de titulares en inglés."""
    if not titulos:
        return 0.0
    pos = neg = 0
    for titulo in titulos:
        palabras = titulo.lower().replace("-", " ").split()
        pos += sum(1 for p in palabras if p in _POSITIVOS)
        neg += sum(1 for p in palabras if p in _NEGATIVOS)
    n = len(titulos)
    raw = (pos - neg) / n
    return max(-1.0, min(1.0, raw))


def obtener_sentimiento(symbol: str) -> float | None:
    """Descarga RSS de CryptoPanic y devuelve score [-1, 1], o None si falla.

    El resultado se cachea _TTL segundos para evitar peticiones repetidas.
    """
    ticker = SYMBOL_TO_TICKER.get(symbol.upper()) or SYMBOL_TO_TICKER.get(symbol)
    if not ticker:
        # Intentar extraer el base currency (ej: "BTC" de "BTCUSDT")
        for quote in ("USDT", "EUR", "USD", "BTC", "ETH"):
            if symbol.upper().endswith(quote):
                ticker = symbol.upper()[:-len(quote)]
                break
    if not ticker:
        _logger.debug("Sin mapeo de ticker para %s", symbol)
        return None

    ts_ahora = time.time()
    cached = _cache.get(ticker)
    if cached is not None and (ts_ahora - cached[0]) < _TTL:
        return cached[1]

    # Backoff: si la fuente falló hace poco, no se reintenta hasta que expire
    # _TTL_FALLO. Evita martillear una fuente caída/403 en cada evaluación (y
    # el spam de logs asociado). Se devuelve el último score conocido o None.
    if ts_ahora < _fallo_hasta.get(ticker, 0.0):
        return cached[1] if cached is not None else None

    url = _RSS_URL.format(ticker=ticker)
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
        titulos: list[str] = []
        # RSS estándar: channel/item/title
        for item in root.iter("item"):
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                titulos.append(title_el.text)
            if len(titulos) >= _MAX_ITEMS:
                break
        score = _puntuar_titulares(titulos)
        _cache[ticker] = (ts_ahora, score)
        _fallo_hasta.pop(ticker, None)  # éxito: se cancela cualquier backoff previo
        _logger.info(
            "Noticias %s: score=%.2f  (%d titulares, pos=%d neg=%d)",
            ticker, score, len(titulos),
            sum(1 for t in titulos for p in t.lower().split() if p in _POSITIVOS),
            sum(1 for t in titulos for p in t.lower().split() if p in _NEGATIVOS),
        )
        return score
    except Exception as exc:
        # Activa el backoff: durante _TTL_FALLO no se reintenta. Como el fetch
        # solo se alcanza cuando el backoff ha expirado, este warning se emite
        # como mucho una vez por ventana (≈30 min) por ticker, no en bucle.
        _fallo_hasta[ticker] = ts_ahora + _TTL_FALLO
        _logger.warning(
            "Noticias no disponibles para %s: %s (no se reintenta en %ds)",
            ticker, exc, _TTL_FALLO,
        )
        # Devolver score cacheado si existe, aunque esté expirado
        if cached is not None:
            return cached[1]
        return None


def noticias_permite_entrada(
    symbol: str,
    umbral_negativo: float = -0.3,
) -> bool | None:
    """Retorna False si el sentimiento de noticias está por debajo del umbral.

    Retorna None si no hay datos disponibles (el llamador no debe bloquear).
    """
    score = obtener_sentimiento(symbol)
    if score is None:
        return None
    if score < umbral_negativo:
        _logger.info(
            "Entrada bloqueada por noticias negativas %s: score=%.2f < umbral=%.2f",
            symbol, score, umbral_negativo,
        )
        return False
    return True
