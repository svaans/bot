from __future__ import annotations

import json
from datetime import datetime, timezone

UTC = timezone.utc
from pathlib import Path

import pytest

import core.risk.riesgo as riesgo


@pytest.fixture(autouse=True)
def redirigir_archivos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    riesgo.detener_persistencia()
    ruta = tmp_path / "riesgo.json"
    backup = tmp_path / "riesgo.json.bak"
    lock = tmp_path / "riesgo.json.lock"
    monkeypatch.setattr(riesgo, "RUTA_ESTADO", str(ruta))
    monkeypatch.setattr(riesgo, "RUTA_ESTADO_BAK", str(backup))
    monkeypatch.setattr(riesgo, "_LOCK_PATH", str(lock))
    yield
    riesgo.detener_persistencia()


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
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.actualizar_perdida("BTCUSDT", -10.0)
    riesgo.esperar_persistencia()
    data = json.loads(Path(riesgo.RUTA_ESTADO).read_text())
    assert data["fecha"] == fecha
    assert data["perdida_acumulada"] == 10.0


def test_actualizar_perdida_agrega_en_memoria(tmp_path: Path) -> None:
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.actualizar_perdida("BTCUSDT", -5.0)
    riesgo.actualizar_perdida("ETHUSDT", -7.5)
    riesgo.esperar_persistencia()
    data = json.loads(Path(riesgo.RUTA_ESTADO).read_text())
    assert data["fecha"] == fecha
    assert pytest.approx(data["perdida_acumulada"], rel=1e-9) == 12.5


def test_riesgo_superado(tmp_path: Path) -> None:
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.guardar_estado_riesgo_seguro({"fecha": fecha, "perdida_acumulada": 50.0})
    assert riesgo.riesgo_superado(0.1, 100.0) is True
    assert riesgo.riesgo_superado(0.6, 100.0) is False
    assert riesgo.riesgo_superado(0.1, 0.0) is False


def test_actualizar_perdida_ignora_ganancias(tmp_path: Path) -> None:
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.guardar_estado_riesgo_seguro({"fecha": fecha, "perdida_acumulada": 5.0})
    riesgo.actualizar_perdida("BTCUSDT", 15.0)
    riesgo.esperar_persistencia()
    data = json.loads(Path(riesgo.RUTA_ESTADO).read_text())
    assert data["perdida_acumulada"] == 5.0


def test_riesgo_superado_fecha_antigua(tmp_path: Path) -> None:
    riesgo.guardar_estado_riesgo_seguro({"fecha": "2000-01-01", "perdida_acumulada": 100.0})
    assert riesgo.riesgo_superado(0.01, 1.0) is False

def test_evaluar_alerta_capital_activa(tmp_path: Path) -> None:
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.guardar_estado_riesgo_seguro({"fecha": fecha, "perdida_acumulada": 50.0})
    activa, ratio = riesgo.evaluar_alerta_capital(100.0, 0.4)
    assert activa is True
    assert pytest.approx(ratio, rel=1e-9) == 0.5


def test_evaluar_alerta_capital_capital_cero(tmp_path: Path) -> None:
    fecha = datetime.now(UTC).date().isoformat()
    riesgo.guardar_estado_riesgo_seguro({"fecha": fecha, "perdida_acumulada": 50.0})
    activa, ratio = riesgo.evaluar_alerta_capital(0.0, 0.5)
    assert activa is False
    assert ratio == 0.0
