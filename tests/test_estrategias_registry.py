"""Coherencia entre ``core.estrategias`` y el loader de entrada."""

from __future__ import annotations

import pytest

from core.diag.auditoria_estrategias import main as auditoria_estrategias_main
from core.estrategias import ESTRATEGIAS_POR_TENDENCIA
from core.strategies.entry.loader import cargar_estrategias


def test_todas_las_estrategias_referenciadas_estan_registradas() -> None:
    pytest.importorskip("ta", reason="Las estrategias cargan indicadores que dependen de `ta`")
    registradas = cargar_estrategias()
    esperadas: set[str] = set()
    for nombres in ESTRATEGIAS_POR_TENDENCIA.values():
        esperadas.update(nombres)
    faltan = esperadas - set(registradas.keys())
    assert not faltan, f"Estrategias sin loader: {sorted(faltan)}"


def test_cli_auditoria_estrategias_exit_zero() -> None:
    pytest.importorskip("ta", reason="Las estrategias cargan indicadores que dependen de `ta`")
    assert auditoria_estrategias_main() == 0
