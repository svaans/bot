"""Rutas seguras para escritura bajo config/ (learning)."""

from __future__ import annotations

from pathlib import Path

import pytest

from learning.config_output_path import (
    _CONFIG_ROOT,
    _REPO_ROOT,
    resolve_config_output_path,
    resolve_repo_input_path,
)


def test_resolve_config_output_path_acepta_ruta_relativa_en_config() -> None:
    p = resolve_config_output_path(Path("config/configuraciones_optimas.json"))
    assert p.is_absolute()
    assert p.parent == _CONFIG_ROOT
    assert "configuraciones_optimas" in p.name


def test_resolve_config_output_path_acepta_absoluta_dentro_de_config() -> None:
    inner = _CONFIG_ROOT / "foo.json"
    p = resolve_config_output_path(inner)
    assert p == inner.resolve()


def test_resolve_config_output_path_rechaza_fuera_de_config() -> None:
    with pytest.raises(ValueError, match="debe estar dentro"):
        resolve_config_output_path(Path("/tmp/evil_weights.json"))


def test_resolve_repo_root_es_padre_de_config() -> None:
    assert _CONFIG_ROOT.is_relative_to(_REPO_ROOT)


def test_resolve_repo_input_path_acepta_dentro_del_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Simula repo con un CSV bajo learning/ usando el mismo mecanismo de raíz."""
    fake_repo = tmp_path / "bot"
    fake_repo.mkdir()
    (fake_repo / "learning").mkdir()
    csv_path = fake_repo / "learning" / "datos.csv"
    csv_path.write_text("a\n1", encoding="utf-8")
    monkeypatch.setattr(
        "learning.config_output_path._REPO_ROOT",
        fake_repo.resolve(),
    )
    monkeypatch.setattr(
        "learning.config_output_path._CONFIG_ROOT",
        (fake_repo / "config").resolve(),
    )
    p = resolve_repo_input_path(Path("learning/datos.csv"))
    assert p == csv_path.resolve()


def test_resolve_repo_input_path_rechaza_fuera_del_repo() -> None:
    with pytest.raises(ValueError, match="debe estar dentro del repositorio"):
        resolve_repo_input_path(Path("/tmp/outside.csv"))
