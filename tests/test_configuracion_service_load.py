"""Carga tolerante de ConfigurationService (archivo ausente / JSON vacío)."""

from __future__ import annotations

from pathlib import Path

from config.configuracion import CONFIG_BASE, ConfigurationService


def test_load_archivo_inexistente_devuelve_config_base(tmp_path: Path) -> None:
    ruta = tmp_path / "no_existe.json"
    assert not ruta.exists()
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("BTC/EUR")
    assert cfg["riesgo_maximo_diario"] == CONFIG_BASE["riesgo_maximo_diario"]
    assert cfg["sl_ratio"] == CONFIG_BASE["sl_ratio"]


def test_load_json_vacio_permite_simbolo_nuevo(tmp_path: Path) -> None:
    ruta = tmp_path / "vacios.json"
    ruta.write_text("{}", encoding="utf-8")
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("ETH/EUR")
    assert cfg["factor_umbral"] == CONFIG_BASE["factor_umbral"]


def test_load_simbolo_parcial_rellena_claves_base(tmp_path: Path) -> None:
    ruta = tmp_path / "mix.json"
    ruta.write_text('{"BTC/EUR": {"sl_ratio": 9.9}}', encoding="utf-8")
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("BTC/EUR")
    assert cfg["sl_ratio"] == 9.9
    assert "riesgo_maximo_diario" in cfg


def test_load_configuracion_no_dict_descarta(tmp_path: Path) -> None:
    ruta = tmp_path / "bad.json"
    ruta.write_text('{"BTC/EUR": "oops"}', encoding="utf-8")
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("BTC/EUR")
    assert cfg["sl_ratio"] == CONFIG_BASE["sl_ratio"]
