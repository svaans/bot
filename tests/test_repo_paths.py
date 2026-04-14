"""core.repo_paths y anclaje de rutas."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.repo_paths import repo_root, resolve_under_repo


def test_repo_root_contiene_core() -> None:
    root = repo_root()
    assert (root / "core" / "repo_paths.py").is_file()


def test_resolve_relativo_bajo_repo() -> None:
    p = resolve_under_repo("config/configuracion.py")
    assert p.is_absolute()
    assert p == (repo_root() / "config" / "configuracion.py").resolve()


def test_resolve_absoluto_sin_prefijo_repo(tmp_path: Path) -> None:
    f = tmp_path / "x.csv"
    f.write_text("a", encoding="utf-8")
    p = resolve_under_repo(f)
    assert p == f.resolve()


@pytest.mark.parametrize(
    ("snapshot", "expected_keys"),
    [
        (None, {"BTC"}),
        ({"eth": {"a": 1.0}}, {"ETH"}),
    ],
)
def test_gestor_pesos_guardar_snapshot(
    snapshot: dict | None,
    expected_keys: set[str],
    tmp_path: Path,
) -> None:
    from core.strategies.pesos import GestorPesos

    path = tmp_path / "pesos.json"
    g = GestorPesos.from_file(path, total=100.0, piso=1.0)
    g.pesos = {"BTC": {"rsi": 50.0}}
    g.guardar(snapshot)
    if snapshot is not None:
        assert set(g.pesos.keys()) == expected_keys
    else:
        assert "BTC" in g.pesos
    assert path.exists()
