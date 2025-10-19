from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from core.persistencia_tecnica import PersistenciaTecnica, coincidencia_parcial


@pytest.mark.parametrize(
    "activations, expected",
    [
        ([(True, False), (True, False), (True, True)], {"A": True}),
        ([(False, False)], {}),
    ],
)
def test_filtrar_persistentes_respects_threshold(activations, expected) -> None:
    persistencia = PersistenciaTecnica(minimo=2)
    resultado = {}
    for idx, (estrategia_a, estrategia_b) in enumerate(activations):
        estrategias = {"A": estrategia_a, "B": estrategia_b}
        resultado = persistencia.filtrar_persistentes("BTC/USDT", estrategias)
        if idx < len(activations) - 1:
            assert "A" in persistencia.conteo["BTC/USDT"]
    assert resultado == expected


def test_export_and_load_state_roundtrip() -> None:
    persistencia = PersistenciaTecnica(minimo=3, peso_extra=0.7)
    persistencia.actualizar("BTC/USDT", {"A": True, "B": False})
    persistencia.actualizar("BTC/USDT", {"A": True, "B": True})
    snapshot = persistencia.export_state()

    restored = PersistenciaTecnica()
    restored.load_state(snapshot)

    assert restored.minimo == 3
    assert restored.peso_extra == pytest.approx(0.7)
    assert restored.conteo == {"BTC/USDT": {"A": 2, "B": 1}}


def test_load_state_handles_invalid_payload() -> None:
    persistencia = PersistenciaTecnica(minimo=5)
    persistencia.load_state({"minimo": "oops", "conteo": {"BTC": {"A": "NaN"}}})
    # Invalid values should be ignored and reset to defaults where applicable
    assert persistencia.minimo == 5
    assert persistencia.conteo == {"BTC": {}}


def test_coincidencia_parcial_scores_recent_matches() -> None:
    historial = [
        {"A": True, "B": False},
        {"A": True, "B": True},
        {"A": False, "B": True},
        {"A": True, "B": True},
        {"A": True, "B": False},
    ]
    pesos = {"A": 2.0, "B": 1.0}
    assert coincidencia_parcial(historial, pesos, ventanas=4) == pytest.approx(2.25)


def test_concurrent_updates_are_thread_safe() -> None:
    persistencia = PersistenciaTecnica(minimo=1)

    def worker(iterations: int) -> None:
        for _ in range(iterations):
            persistencia.actualizar("BTC/USDT", {"A": True, "B": False})

    iterations = 500
    workers = 4
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, iterations) for _ in range(workers)]
        for future in futures:
            future.result()

    assert persistencia.conteo["BTC/USDT"]["A"] == iterations * workers
    assert persistencia.conteo["BTC/USDT"]["B"] == 0
    assert persistencia.es_persistente("BTC/USDT", "A")