"""Política y rutas de persistencia de pesos de entrada."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.strategies.pesos import ESTRATEGIAS_PESOS_PATH, GestorPesos, entry_weights_temp_path


def test_entry_weights_temp_path_junto_a_canonico() -> None:
    t = entry_weights_temp_path()
    assert t.name == f"{ESTRATEGIAS_PESOS_PATH.name}.tmp"
    assert t.parent == ESTRATEGIAS_PESOS_PATH.parent


def test_persist_entry_weights_respeta_PESOS_ENTRY_ONLY_SOURCE(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights

    monkeypatch.setenv("PESOS_ENTRY_ONLY_SOURCE", EntryWeightSource.RECALIBRAR_SEMANA)
    path = tmp_path / "pesos.json"
    g = GestorPesos.from_file(path, total=100.0, piso=1.0)
    g.pesos = {"BTC": {"rsi": 10.0}}

    persist_entry_weights(
        g,
        None,
        source=EntryWeightSource.ENTRENADOR_SIMBOLO,
        detail="test",
    )
    assert not path.exists()

    persist_entry_weights(
        g,
        None,
        source=EntryWeightSource.RECALIBRAR_SEMANA,
        detail="test",
    )
    assert path.exists()
