"""Regresión: capital / notional alineado con el quote de SYMBOLS (p. ej. USDT)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import config.config_manager as cm
from core.capital_manager import CapitalManager
from core.capital_repository import CapitalRepository


def _paper_env_base(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOT_ENV", "development")
    monkeypatch.setenv("MODO_REAL", "false")
    monkeypatch.setattr(cm, "load_dotenv", lambda *_a, **_k: None, raising=False)


def test_capital_currency_inferred_usdt_when_unset_and_symbols_usdt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _paper_env_base(monkeypatch)
    monkeypatch.setenv("SYMBOLS", "BTC/USDT,ETH/USDT")
    monkeypatch.delenv("CAPITAL_CURRENCY", raising=False)
    cfg = cm.ConfigManager.load_from_env()
    assert cfg.capital_currency == "USDT"


def test_capital_currency_explicit_usdt_with_usdt_symbols_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _paper_env_base(monkeypatch)
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.setenv("CAPITAL_CURRENCY", "usdt")
    cfg = cm.ConfigManager.load_from_env()
    assert cfg.capital_currency == "USDT"


def test_capital_currency_eur_with_usdt_symbols_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _paper_env_base(monkeypatch)
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.setenv("CAPITAL_CURRENCY", "EUR")
    with pytest.raises(ValueError, match="CAPITAL_CURRENCY=EUR"):
        cm.ConfigManager.load_from_env()


def test_capital_currency_inferred_eur_when_symbols_only_eur_quote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _paper_env_base(monkeypatch)
    monkeypatch.setenv("SYMBOLS", "BTC/EUR,ETH/EUR")
    monkeypatch.delenv("CAPITAL_CURRENCY", raising=False)
    cfg = cm.ConfigManager.load_from_env()
    assert cfg.capital_currency == "EUR"


def test_min_order_quote_env_takes_precedence_over_min_order_eur_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _paper_env_base(monkeypatch)
    monkeypatch.setenv("SYMBOLS", "BTC/USDT")
    monkeypatch.setenv("MIN_ORDER_QUOTE", "47")
    monkeypatch.delenv("MIN_ORDER_EUR", raising=False)
    cfg = cm.ConfigManager.load_from_env()
    assert cfg.min_order_eur == pytest.approx(47.0)


def test_capital_manager_bootstrap_min_order_is_quote_notional_for_usdt_pairs(
    tmp_path: Path,
) -> None:
    """Sin RISK_*, el arranque usa ``min_order_eur`` como presupuesto notional en quote."""
    config = SimpleNamespace(
        symbols=["BTC/USDT", "ETH/USDT"],
        risk_capital_total=0.0,
        risk_capital_default_per_symbol=0.0,
        risk_capital_per_symbol={},
        min_order_eur=15.0,
        risk_kelly_base=0.1,
    )
    repo = CapitalRepository(path=tmp_path / "capital_quote.json")
    capital = CapitalManager(config, capital_repository=repo)
    assert capital.exposure_disponible("BTC/USDT") == pytest.approx(15.0)
    assert capital.exposure_disponible("ETH/USDT") == pytest.approx(15.0)
    out = capital.calcular_cantidad_async("BTC/USDT", 30_000.0)
    assert out["cantidad"] == pytest.approx(15.0 / 30_000.0)
