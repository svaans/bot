"""Regresión: defaults de config alineados con DataFeed y paper Binance spot."""

from __future__ import annotations

import pytest

from config.development import DevelopmentConfig
from config.production import ProductionConfig
from core.data_feed.datafeed import DataFeed


def test_development_config_handler_timeout_matches_datafeed_default() -> None:
    assert DevelopmentConfig().handler_timeout == 180.0


def test_datafeed_constructor_default_handler_timeout_is_180() -> None:
    feed = DataFeed("5m")
    assert feed.handler_timeout == 180.0


def test_development_default_symbols_are_binance_spot_usdt() -> None:
    assert DevelopmentConfig().symbols == ["BTC/USDT", "ETH/USDT"]


def test_production_default_symbols_match_development_spot_usdt() -> None:
    assert ProductionConfig().symbols == ["BTC/USDT", "ETH/USDT"]


def test_config_manager_respects_symbols_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    import config.config_manager as cm

    monkeypatch.setenv("BOT_ENV", "development")
    monkeypatch.setenv("MODO_REAL", "false")
    monkeypatch.setenv("SYMBOLS", "SOL/USDT,ADA/USDT")
    monkeypatch.setattr(cm, "load_dotenv", lambda *_a, **_k: None, raising=False)
    cfg = cm.ConfigManager.load_from_env()
    assert cfg.symbols == ["SOL/USDT", "ADA/USDT"]
    assert cfg.capital_currency == "USDT"
