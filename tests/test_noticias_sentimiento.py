"""Tests unitarios para core/strategies/noticias_sentimiento.py."""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch


from core.strategies.noticias_sentimiento import (
    _puntuar_titulares,
    noticias_permite_entrada,
    obtener_sentimiento,
    _cache,
)


def _make_rss(titles: list[str]) -> bytes:
    """Construye un RSS mínimo con los títulos dados."""
    root = ET.Element("rss")
    channel = ET.SubElement(root, "channel")
    for t in titles:
        item = ET.SubElement(channel, "item")
        title_el = ET.SubElement(item, "title")
        title_el.text = t
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _mock_urlopen(content: bytes):
    """Devuelve un context manager que simula urllib.request.urlopen."""
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=MagicMock(read=MagicMock(return_value=content)))
    cm.__exit__ = MagicMock(return_value=False)
    return cm


# ── tests de scoring puro ──────────────────────────────────────────────────

def test_puntuar_titulares_positivo():
    score = _puntuar_titulares(["Bitcoin rally surge bullish", "ETH breakout record"])
    assert score > 0


def test_puntuar_titulares_negativo():
    score = _puntuar_titulares(["BTC crash bearish dump", "Hack stolen exploit"])
    assert score < 0


def test_puntuar_titulares_neutro():
    score = _puntuar_titulares(["BTC trades at 40000", "Market update"])
    assert score == 0.0


def test_puntuar_titulares_vacio():
    assert _puntuar_titulares([]) == 0.0


def test_puntuar_titulares_clamped():
    titulos = ["crash hack bearish dump collapse"] * 20
    score = _puntuar_titulares(titulos)
    assert score >= -1.0


# ── tests de obtener_sentimiento ──────────────────────────────────────────

def test_obtener_sentimiento_positivo():
    _cache.clear()
    rss = _make_rss(["Bitcoin rally to ATH", "Institutional adoption surge", "BTC breakout record"])
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(rss)):
        score = obtener_sentimiento("BTC/USDT")
    assert score is not None
    assert score > 0


def test_obtener_sentimiento_negativo():
    _cache.clear()
    rss = _make_rss(["Bitcoin crash bearish", "BTC hack stolen funds", "Market dump collapse"])
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(rss)):
        score = obtener_sentimiento("ETH/USDT")
    assert score is not None
    assert score < 0


def test_obtener_sentimiento_cache_no_refetch():
    """Segunda llamada usa cache sin hacer HTTP."""
    _cache.clear()
    rss = _make_rss(["Bitcoin rally"])
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(rss)) as mock_url:
        obtener_sentimiento("SOL/USDT")
        obtener_sentimiento("SOL/USDT")
    assert mock_url.call_count == 1


def test_obtener_sentimiento_falla_retorna_none():
    _cache.clear()
    with patch("urllib.request.urlopen", side_effect=Exception("timeout")):
        score = obtener_sentimiento("XRP/USDT")
    assert score is None


def test_obtener_sentimiento_falla_usa_cache_expirado():
    """Si hay un valor cacheado aunque expirado y falla el fetch, lo devuelve."""
    _cache.clear()
    _cache["AVAX"] = (time.time() - 7200, 0.5)  # expirado
    with patch("urllib.request.urlopen", side_effect=Exception("timeout")):
        score = obtener_sentimiento("AVAX/USDT")
    assert score == 0.5


def test_obtener_sentimiento_simbolo_sin_mapping():
    _cache.clear()
    score = obtener_sentimiento("UNKNOWN/PAIR")
    assert score is None


# ── tests de noticias_permite_entrada ────────────────────────────────────

def test_permite_entrada_positivo_permite():
    _cache.clear()
    rss = _make_rss(["Bitcoin surge rally adoption"])
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(rss)):
        resultado = noticias_permite_entrada("BTC/USDT", umbral_negativo=-0.3)
    assert resultado is True


def test_permite_entrada_negativo_bloquea():
    _cache.clear()
    rss = _make_rss(["BTC crash hack bearish dump", "collapse fraud stolen"] * 5)
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(rss)):
        resultado = noticias_permite_entrada("ETH/USDT", umbral_negativo=-0.3)
    assert resultado is False


def test_permite_entrada_none_no_bloquea():
    """Si no hay datos, retorna None (el motor no debe bloquear)."""
    _cache.clear()
    with patch("urllib.request.urlopen", side_effect=Exception("network")):
        resultado = noticias_permite_entrada("SOL/USDT")
    assert resultado is None
