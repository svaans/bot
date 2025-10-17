from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.capital_manager import CapitalManager
from core.capital_repository import CapitalRepository


class _Config:
    symbols = ["BTCUSDT", "ETHUSDT"]
    risk_capital_total = 200.0
    risk_capital_default_per_symbol = 0.0
    risk_capital_per_symbol = {"BTCUSDT": 120.0, "ETHUSDT": 80.0}
    risk_kelly_base = 0.1
    min_order_eur = 10.0
    risk_capital_divergence_threshold = 0.25


def _create_manager(tmp_path: Path) -> tuple[CapitalManager, Path]:
    repo_path = tmp_path / "estado" / "capital.json"
    repo = CapitalRepository(path=repo_path)
    manager = CapitalManager(_Config(), capital_repository=repo)
    return manager, repo_path


def test_capital_manager_persists_open_operation(tmp_path: Path) -> None:
    manager, repo_path = _create_manager(tmp_path)

    comprometido = 50.0
    asignado = manager.exposure_asignada("BTCUSDT")
    manager.actualizar_exposure("BTCUSDT", max(asignado - comprometido, 0.0))

    assert manager.exposure_disponible("BTCUSDT") == pytest.approx(70.0)
    assert manager.exposure_disponible() == pytest.approx(150.0)

    contenido = json.loads(repo_path.read_text(encoding="utf-8"))
    assert contenido["capital_por_simbolo"]["BTCUSDT"] == pytest.approx(70.0)
    assert contenido["capital_por_simbolo"]["ETHUSDT"] == pytest.approx(80.0)
    assert contenido["disponible_global"] == pytest.approx(150.0)

    reopened = CapitalManager(
        _Config(), capital_repository=CapitalRepository(path=repo_path)
    )
    assert reopened.exposure_disponible("BTCUSDT") == pytest.approx(70.0)
    assert reopened.exposure_asignada("BTCUSDT") == pytest.approx(70.0)
    assert reopened.exposure_disponible() == pytest.approx(150.0)


def test_capital_manager_persists_close_operation(tmp_path: Path) -> None:
    manager, repo_path = _create_manager(tmp_path)

    comprometido = 50.0
    asignado = manager.exposure_asignada("BTCUSDT")
    manager.actualizar_exposure("BTCUSDT", max(asignado - comprometido, 0.0))

    reopened = CapitalManager(
        _Config(), capital_repository=CapitalRepository(path=repo_path)
    )
    reopened.actualizar_exposure(
        "BTCUSDT", _Config.risk_capital_per_symbol["BTCUSDT"]
    )

    assert reopened.exposure_disponible("BTCUSDT") == pytest.approx(120.0)
    assert reopened.exposure_disponible() == pytest.approx(200.0)

    contenido = json.loads(repo_path.read_text(encoding="utf-8"))
    assert contenido["capital_por_simbolo"]["BTCUSDT"] == pytest.approx(120.0)
    assert contenido["capital_por_simbolo"]["ETHUSDT"] == pytest.approx(80.0)
    assert contenido["disponible_global"] == pytest.approx(200.0)

    recargado = CapitalManager(
        _Config(), capital_repository=CapitalRepository(path=repo_path)
    )
    assert recargado.exposure_disponible("BTCUSDT") == pytest.approx(120.0)
    assert recargado.exposure_asignada("BTCUSDT") == pytest.approx(120.0)
    assert recargado.exposure_disponible() == pytest.approx(200.0)