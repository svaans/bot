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
    # [FIX CAPITAL-CONFIG-OVERRIDE] exposure_asignada debe devolver el máximo
    # configurado (120), no el valor del snapshot (70). El snapshot representa
    # capital disponible (runtime), no el techo de configuración.
    assert reopened.exposure_asignada("BTCUSDT") == pytest.approx(120.0)
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


def test_capital_config_override_snapshot_does_not_exceed_configured_max(
    tmp_path: Path,
) -> None:
    """CAPITAL-CONFIG-OVERRIDE: snapshot con valor alto no supera el límite de config.

    Si el operador reduce el límite de BTCUSDT de 120 → 80 y el snapshot
    conserva el valor anterior (120), tras el reinicio capital disponible
    debe ser min(120, 80) = 80, no 120.
    """
    repo_path = tmp_path / "estado" / "capital.json"
    repo = CapitalRepository(path=repo_path)

    # Snapshot con valor mayor que el nuevo límite de config
    repo.save({"BTCUSDT": 120.0, "ETHUSDT": 80.0}, disponible_global=200.0)

    class _ReducedConfig:
        symbols = ["BTCUSDT", "ETHUSDT"]
        risk_capital_total = 160.0
        risk_capital_default_per_symbol = 0.0
        risk_capital_per_symbol = {"BTCUSDT": 80.0, "ETHUSDT": 80.0}  # reducido de 120→80
        risk_kelly_base = 0.1
        min_order_eur = 10.0
        risk_capital_divergence_threshold = 0.0

    manager = CapitalManager(_ReducedConfig(), capital_repository=CapitalRepository(path=repo_path))

    # Disponible debe estar capado al nuevo máximo configurado
    assert manager.exposure_disponible("BTCUSDT") == pytest.approx(80.0)
    # El techo configurado es el nuevo límite, no el snapshot
    assert manager.exposure_asignada("BTCUSDT") == pytest.approx(80.0)


def test_capital_config_override_close_after_restart_restores_full_configured_capital(
    tmp_path: Path,
) -> None:
    """CAPITAL-CONFIG-OVERRIDE: el cierre tras reinicio restaura el máximo configurado.

    Escenario de pérdida silenciosa original:
    1. Config: BTCUSDT = 120 EUR
    2. Trade abre → disponible baja a 70 EUR → snapshot guarda 70
    3. Bot reinicia con snapshot {BTCUSDT: 70}
    4. Trade cierra → sincronizar_exposure("BTCUSDT", comprometido=0)
       Antes del fix: asignado = exposure_asignada = 70 → restaura 70, no 120 → 50 EUR perdidos
       Tras el fix:   asignado = exposure_asignada = 120 → restaura 120 → capital completo
    """
    repo_path = tmp_path / "estado" / "capital.json"

    # Simula snapshot tras apertura de trade: BTCUSDT bajó de 120 → 70
    repo = CapitalRepository(path=repo_path)
    repo.save({"BTCUSDT": 70.0, "ETHUSDT": 80.0}, disponible_global=150.0)

    # Reinicio con misma config (120)
    reopened = CapitalManager(_Config(), capital_repository=CapitalRepository(path=repo_path))

    assert reopened.exposure_disponible("BTCUSDT") == pytest.approx(70.0)  # snapshot preservado
    assert reopened.exposure_asignada("BTCUSDT") == pytest.approx(120.0)   # config, no snapshot

    # Cierre del trade: sincronizar_exposure(symbol, comprometido=0)
    asignado = reopened.exposure_asignada("BTCUSDT")   # debe ser 120
    disponible_tras_cierre = max(asignado - 0.0, 0.0)  # 120
    reopened.actualizar_exposure("BTCUSDT", disponible_tras_cierre)

    assert reopened.exposure_disponible("BTCUSDT") == pytest.approx(120.0)  # capital completo recuperado
    assert reopened.exposure_disponible() == pytest.approx(200.0)