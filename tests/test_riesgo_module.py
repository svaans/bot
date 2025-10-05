from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

import core.risk.riesgo as riesgo


@pytest.fixture(autouse=True)
def redirigir_archivos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ruta = tmp_path / "riesgo.json"
    backup = tmp_path / "riesgo.json.bak"
    lock = tmp_path / "riesgo.json.lock"
    monkeypatch.setattr(riesgo, "RUTA_ESTADO", str(ruta))
    monkeypatch.setattr(riesgo, "RUTA_ESTADO_BAK", str(backup))
    monkeypatch.setattr(riesgo, "_LOCK_PATH", str(lock))


def test_cargar_estado_por_defecto(tmp_path: Path) -> None:
    estado = riesgo.cargar_estado_riesgo_seguro()
    assert estado == {"fecha": "", "perdida_acumulada": 0.0}


def test_cargar_desde_backup(tmp_path: Path) -> None:
    backup = Path(riesgo.RUTA_ESTADO_BAK)
    backup.write_text(json.dumps({"fecha": "2024-01-01", "perdida_acumulada": 10.0}))
    estado = riesgo.cargar_estado_riesgo_seguro()
    assert estado["perdida_acumulada"] == 10.0


def test_guardar_estado(tmp_path: Path) -> None:
    data = {"fecha": "2024-01-02", "perdida_acumulada": 5.0}
    riesgo.guardar_estado_riesgo_seguro(data)

    ruta = Path(riesgo.RUTA_ESTADO)
    backup = Path(riesgo.RUTA_ESTADO_BAK)

    assert ruta.exists()
    assert backup.exists()
    cargado = json.loads(ruta.read_text())
    assert cargado == data


def test_actualizar_perdida(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fecha = datetime.utcnow().date().isoformat()
    riesgo.actualizar_perdida("BTCUSDT", -10.0)
    data = json.loads(Path(riesgo.RUTA_ESTADO).read_text())
    assert data["fecha"] == fecha
    assert data["perdida_acumulada"] == 10.0


def test_riesgo_superado(tmp_path: Path) -> None:
    fecha = datetime.utcnow().date().isoformat()
    riesgo.guardar_estado_riesgo_seguro({"fecha": fecha, "perdida_acumulada": 50.0})
    assert riesgo.riesgo_superado(0.1, 100.0) is True
    assert riesgo.riesgo_superado(0.6, 100.0) is False
    assert riesgo.riesgo_superado(0.1, 0.0) is False