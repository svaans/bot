from __future__ import annotations

from pathlib import Path

import pytest

from core.capital_repository import CapitalRepository


def test_capital_repository_roundtrip(tmp_path: Path) -> None:
    repo = CapitalRepository(path=tmp_path / "capital.json")
    repo.save({"btc/usdt": 150.5, "ETHUSDT": -20.0}, 175.25)

    snapshot = repo.load()

    assert snapshot.capital_por_simbolo["BTC/USDT"] == pytest.approx(150.5)
    assert snapshot.capital_por_simbolo["ETHUSDT"] == pytest.approx(0.0)
    assert snapshot.disponible_global == pytest.approx(175.25)


def test_capital_repository_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "capital.json"
    path.write_text("{", encoding="utf-8")

    repo = CapitalRepository(path=path)
    snapshot = repo.load()

    assert snapshot.capital_por_simbolo == {}
    assert snapshot.disponible_global == pytest.approx(0.0)
