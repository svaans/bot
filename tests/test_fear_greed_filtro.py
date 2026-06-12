"""Tests del Fear & Greed Index filter (core/strategies/filtro_macro.py).

Solo importa filtro_macro.py, sin StrategyEngine ni librería `ta`.
Puede ejecutarse en cualquier entorno con: pytest tests/test_fear_greed_filtro.py --noconftest
"""
from __future__ import annotations

from unittest.mock import patch


from core.strategies.filtro_macro import fear_greed_permite_entrada, obtener_fear_greed


# ─── tests de fear_greed_permite_entrada (función pura con mock) ─────────────

def test_permite_entrada_valor_bajo_umbral() -> None:
    """F&G=50 con umbral=75 → permite entrada."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=50):
        assert fear_greed_permite_entrada(umbral_codicia=75) is True


def test_bloquea_valor_sobre_umbral() -> None:
    """F&G=80 con umbral=75 → bloquea entrada."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=80):
        assert fear_greed_permite_entrada(umbral_codicia=75) is False


def test_exactamente_en_umbral_permite() -> None:
    """F&G=75 con umbral=75 → permite (umbral no es estricto)."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=75):
        assert fear_greed_permite_entrada(umbral_codicia=75) is True


def test_sin_datos_devuelve_none() -> None:
    """Sin datos de F&G → None (el llamador no debe bloquear)."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=None):
        assert fear_greed_permite_entrada(umbral_codicia=75) is None


def test_umbral_personalizado_estricto() -> None:
    """Con umbral=60: F&G=65 bloquea, F&G=55 permite."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=65):
        assert fear_greed_permite_entrada(umbral_codicia=60) is False
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=55):
        assert fear_greed_permite_entrada(umbral_codicia=60) is True


def test_umbral_extremo_100_siempre_permite() -> None:
    """Con umbral=100 cualquier valor real siempre permite."""
    for valor in (0, 25, 50, 75, 99, 100):
        with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=valor):
            assert fear_greed_permite_entrada(umbral_codicia=100) is True


def test_umbral_0_siempre_bloquea_si_hay_datos() -> None:
    """Con umbral=0 cualquier valor positivo bloquea (F&G siempre > 0)."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=1):
        assert fear_greed_permite_entrada(umbral_codicia=0) is False


# ─── tests de zona_neutral (umbral_miedo) ────────────────────────────────────

def test_zona_neutral_bloquea_panico_extremo() -> None:
    """F&G=15 (pánico) con umbral_miedo=25 → bloquea entrada."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=15):
        assert fear_greed_permite_entrada(umbral_codicia=75, umbral_miedo=25) is False


def test_zona_neutral_permite_zona_media() -> None:
    """F&G=50 (neutral) con zona 25-75 → permite entrada."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=50):
        assert fear_greed_permite_entrada(umbral_codicia=75, umbral_miedo=25) is True


def test_zona_neutral_bloquea_codicia_extrema() -> None:
    """F&G=80 (codicia) con zona 25-75 → bloquea entrada."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=80):
        assert fear_greed_permite_entrada(umbral_codicia=75, umbral_miedo=25) is False


def test_zona_neutral_exactamente_en_limite_inferior_permite() -> None:
    """F&G=25 con umbral_miedo=25 → permite (límite no es estricto)."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=25):
        assert fear_greed_permite_entrada(umbral_codicia=75, umbral_miedo=25) is True


def test_umbral_miedo_cero_no_bloquea_panico() -> None:
    """umbral_miedo=0 (desactivado) no bloquea aunque F&G sea muy bajo."""
    with patch("core.strategies.filtro_macro.obtener_fear_greed", return_value=5):
        assert fear_greed_permite_entrada(umbral_codicia=75, umbral_miedo=0) is True


# ─── tests de obtener_fear_greed (caché en memoria) ──────────────────────────

def test_obtener_fear_greed_usa_cache() -> None:
    """Una segunda llamada dentro del TTL debe devolver el valor cacheado sin HTTP."""
    import core.strategies.filtro_macro as fm
    import time

    fm._fg_cache = (time.time(), 42)
    resultado = obtener_fear_greed()
    assert resultado == 42


def test_obtener_fear_greed_cache_expirado_intenta_fetch() -> None:
    """Con caché expirado debe intentar una petición HTTP (mocked)."""
    import core.strategies.filtro_macro as fm

    fm._fg_cache = (0.0, None)  # expirado
    mock_data = {"data": [{"value": "55", "value_classification": "Greed"}]}
    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value.__enter__.return_value.read.return_value = b'{}'
        mock_response = mock_url.return_value.__enter__.return_value
        mock_response.__iter__ = lambda s: iter([])
        # Simulamos urlopen devolviendo JSON válido
        import json as _json
        mock_response_obj = type("R", (), {
            "read": lambda self: _json.dumps(mock_data).encode(),
            "__enter__": lambda self: self,
            "__exit__": lambda self, *a: None,
        })()
        mock_url.return_value = mock_response_obj
        resultado = obtener_fear_greed()
    assert resultado == 55


def test_obtener_fear_greed_fallo_http_devuelve_cache_anterior() -> None:
    """Si la petición HTTP falla, devuelve el valor previo en caché."""
    import core.strategies.filtro_macro as fm

    fm._fg_cache = (0.0, 33)  # caché expirado pero con valor previo
    with patch("urllib.request.urlopen", side_effect=OSError("no internet")):
        resultado = obtener_fear_greed()
    assert resultado == 33
