"""Fase 14 — Regresión: order_circuit_* propagados desde Config a través de load_from_env().

Antes del fix, Config no tenía los 3 campos order_circuit_* aunque DevelopmentConfig los
definía. _resolve_circuit_params (circuit_breaker.py) los leía vía getattr con fallback al
default de módulo, pero nunca podía recibir valores configurados vía Config.

El fix:
  1. Añade order_circuit_max_failures / open_seconds / reset_after a Config.
  2. load_from_env() lee ORDER_CIRCUIT_* env vars y los pasa al constructor de Config.
"""
from __future__ import annotations

import pytest

from config.development import DevelopmentConfig
from config.production import ProductionConfig


# ---------------------------------------------------------------------------
# Valores por defecto en DevelopmentConfig
# ---------------------------------------------------------------------------

def test_development_config_order_circuit_defaults() -> None:
    cfg = DevelopmentConfig()
    assert cfg.order_circuit_max_failures == 3
    assert cfg.order_circuit_open_seconds == 30.0
    assert cfg.order_circuit_reset_after == 120.0


def test_production_config_inherits_order_circuit_defaults() -> None:
    """ProductionConfig hereda estos valores de DevelopmentConfig."""
    cfg = ProductionConfig()
    assert cfg.order_circuit_max_failures == 3
    assert cfg.order_circuit_open_seconds == 30.0
    assert cfg.order_circuit_reset_after == 120.0


# ---------------------------------------------------------------------------
# Config frozen dataclass tiene los campos (no AttributeError)
# ---------------------------------------------------------------------------

def test_config_has_order_circuit_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    import config.config_manager as cm

    monkeypatch.setenv("BOT_ENV", "development")
    monkeypatch.setenv("MODO_REAL", "false")
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.delenv("CAPITAL_CURRENCY", raising=False)
    monkeypatch.setattr(cm, "load_dotenv", lambda *_a, **_k: None, raising=False)

    cfg = cm.ConfigManager.load_from_env()

    # Estos campos deben existir (antes del fix lanzaban AttributeError)
    assert hasattr(cfg, "order_circuit_max_failures")
    assert hasattr(cfg, "order_circuit_open_seconds")
    assert hasattr(cfg, "order_circuit_reset_after")


def test_config_order_circuit_defaults_match_development_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import config.config_manager as cm

    monkeypatch.setenv("BOT_ENV", "development")
    monkeypatch.setenv("MODO_REAL", "false")
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.delenv("CAPITAL_CURRENCY", raising=False)
    monkeypatch.delenv("ORDER_CIRCUIT_MAX_FAILURES", raising=False)
    monkeypatch.delenv("ORDER_CIRCUIT_OPEN_SECONDS", raising=False)
    monkeypatch.delenv("ORDER_CIRCUIT_RESET_AFTER", raising=False)
    monkeypatch.setattr(cm, "load_dotenv", lambda *_a, **_k: None, raising=False)

    cfg = cm.ConfigManager.load_from_env()

    assert cfg.order_circuit_max_failures == DevelopmentConfig().order_circuit_max_failures
    assert cfg.order_circuit_open_seconds == DevelopmentConfig().order_circuit_open_seconds
    assert cfg.order_circuit_reset_after == DevelopmentConfig().order_circuit_reset_after


# ---------------------------------------------------------------------------
# Env var overrides son respetados
# ---------------------------------------------------------------------------

def test_order_circuit_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    import config.config_manager as cm

    monkeypatch.setenv("BOT_ENV", "development")
    monkeypatch.setenv("MODO_REAL", "false")
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.delenv("CAPITAL_CURRENCY", raising=False)
    monkeypatch.setenv("ORDER_CIRCUIT_MAX_FAILURES", "7")
    monkeypatch.setenv("ORDER_CIRCUIT_OPEN_SECONDS", "60.0")
    monkeypatch.setenv("ORDER_CIRCUIT_RESET_AFTER", "300.0")
    monkeypatch.setattr(cm, "load_dotenv", lambda *_a, **_k: None, raising=False)

    cfg = cm.ConfigManager.load_from_env()

    assert cfg.order_circuit_max_failures == 7
    assert cfg.order_circuit_open_seconds == pytest.approx(60.0)
    assert cfg.order_circuit_reset_after == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# _resolve_circuit_params lee desde Config (integración mínima)
# ---------------------------------------------------------------------------

def test_resolve_circuit_params_reads_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """_resolve_circuit_params devuelve los valores de config cuando Config los tiene."""
    from core.vela.circuit_breaker import _resolve_circuit_params

    class _FakeConfig:
        order_circuit_max_failures = 10
        order_circuit_open_seconds = 45.0
        order_circuit_reset_after = 200.0

    class _FakeTrader:
        config = _FakeConfig()

    max_f, open_s, reset = _resolve_circuit_params(_FakeTrader())

    assert max_f == 10
    assert open_s == pytest.approx(45.0)
    assert reset == pytest.approx(200.0)
