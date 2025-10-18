"""Tests para la estrategia de momentum y su umbral configurable."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import core.strategies.entry.estrategia_momentum as estrategia_mod
from indicadores import helpers as helpers_mod
from indicadores import settings as settings_mod
from indicadores.settings import _reset_indicator_settings_cache_for_tests


@pytest.fixture(autouse=True)
def reset_indicator_settings() -> None:
    """Resetea la caché global de configuración tras cada prueba."""

    _reset_indicator_settings_cache_for_tests()


def _build_dataframe(rows: int = 20) -> pd.DataFrame:
    data = {
        "timestamp": list(range(rows)),
        "open": [100.0] * rows,
        "high": [101.0] * rows,
        "low": [99.0] * rows,
        "close": [100.0 + (i * 0.01) for i in range(rows)],
        "volume": [10.0] * rows,
    }
    return pd.DataFrame(data)


def test_estrategia_momentum_neutral_zone(monkeypatch: pytest.MonkeyPatch) -> None:
    df = _build_dataframe()
    df.attrs["symbol"] = "BTCUSDT"

    monkeypatch.setattr(estrategia_mod, "get_momentum", lambda _df: 0.0005)
    monkeypatch.setattr(estrategia_mod, "resolve_momentum_threshold", lambda _symbol: 0.001)

    resultado = estrategia_mod.estrategia_momentum(df)

    assert resultado["activo"] is False
    assert "neutra" in resultado["mensaje"]


def test_estrategia_momentum_override_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    df = _build_dataframe()
    df.attrs["symbol"] = "ADAUSDT"

    monkeypatch.setattr(estrategia_mod, "get_momentum", lambda _df: 0.0003)
    monkeypatch.setattr(
        estrategia_mod,
        "resolve_momentum_threshold",
        lambda symbol: 0.0002 if str(symbol).upper() == "ADAUSDT" else 0.001,
    )

    resultado = estrategia_mod.estrategia_momentum(df)

    assert resultado["activo"] is True
    assert "positivo" in resultado["mensaje"]


def test_resolve_momentum_threshold_override(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_cfg = SimpleNamespace(
        indicadores_normalize_default=True,
        indicadores_cache_max_entries=256,
        momentum_activation_threshold=0.003,
        momentum_threshold_overrides={"ADAUSDT": 0.0004, "BTCUSDT": 0.0008},
    )

    class DummyManager:
        @staticmethod
        def load_from_env() -> SimpleNamespace:
            return dummy_cfg

    monkeypatch.setattr(settings_mod, "ConfigManager", DummyManager)

    assert helpers_mod.resolve_momentum_threshold() == pytest.approx(0.003)
    assert helpers_mod.resolve_momentum_threshold("ADAUSDT") == pytest.approx(0.0004)
    assert helpers_mod.resolve_momentum_threshold("adausdt") == pytest.approx(0.0004)
    assert helpers_mod.resolve_momentum_threshold("BTCUSDT") == pytest.approx(0.0008)