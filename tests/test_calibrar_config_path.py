"""Rutas seguras para escritura bajo config/ (learning)."""

from __future__ import annotations

from pathlib import Path

import pytest

from learning.config_output_path import _CONFIG_ROOT, _REPO_ROOT, resolve_config_output_path


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
