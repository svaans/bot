"""Tests for orphan_reconciler — reconciliación de intents huérfanos."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

# Reset the global CCXT_READY event before each test to avoid cross-test pollution
import core.orders.orphan_reconciler as orphan_mod


@pytest.fixture(autouse=True)
def reset_ccxt_ready_event() -> None:
    """Resetea el Event global _CCXT_READY entre tests para evitar contaminación."""
    orphan_mod._CCXT_READY = None
    yield
    orphan_mod._CCXT_READY = None


# ---------------------------------------------------------------------------
# ORPHAN-ASYNCIO-WAIT-LEAK-01: tasks internas canceladas correctamente
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_all_cancels_pending_wait_tasks_on_ccxt_ready() -> None:
    """ORPHAN-ASYNCIO-WAIT-LEAK-01: cuando ccxt_ready dispara, wait_shutdown se cancela.

    Antes del fix, wait_shutdown quedaba como task pendiente huérfana produciendo
    "Task destroyed but pending" warnings en el cierre del loop.
    """
    from core.orders.orphan_reconciler import OrphanReconciler, get_ccxt_ready_event

    reconciler = OrphanReconciler()

    # Inyectar un record ficticio para que _reconcile_all no termine inmediatamente
    # en el gather (usamos un mock que no hace nada)
    reconcile_calls: list[str] = []

    async def fake_reconcile_one(rec: object) -> None:
        reconcile_calls.append(getattr(rec, "operation_id", "?"))

    reconciler._reconcile_one = fake_reconcile_one  # type: ignore[method-assign]

    from core.orders.orphan_reconciler import OrphanRecord, OrphanState

    rec = OrphanRecord(
        file=Path("/tmp/fake_intent.json"),
        symbol="BTCUSDT",
        operation_id="op-test",
        data={},
        state=OrphanState.DETECTED,
    )
    reconciler._records["op-test"] = rec

    # Lanzar _reconcile_all como task
    task = asyncio.create_task(reconciler._reconcile_all())

    # Breve pausa para que llegue al await asyncio.wait(...)
    await asyncio.sleep(0.01)

    # Señalar ccxt ready → wait_ccxt completa, wait_shutdown debe cancelarse
    get_ccxt_ready_event().set()

    # Esperar a que _reconcile_all termine
    await asyncio.wait_for(task, timeout=2.0)

    # Dar un ciclo al loop para que las tasks canceladas finalicen su cleanup
    await asyncio.sleep(0)

    # Verificar que no quedan tasks huérfanas nombradas wait_ccxt / wait_shutdown
    all_tasks = asyncio.all_tasks()
    pending_names = {t.get_name() for t in all_tasks}
    assert "wait_ccxt" not in pending_names, (
        "wait_ccxt task sigue pendiente — ORPHAN-ASYNCIO-WAIT-LEAK-01 no corregido"
    )
    assert "wait_shutdown" not in pending_names, (
        "wait_shutdown task sigue pendiente — ORPHAN-ASYNCIO-WAIT-LEAK-01 no corregido"
    )


@pytest.mark.asyncio
async def test_reconcile_all_cancels_wait_ccxt_on_shutdown() -> None:
    """ORPHAN-ASYNCIO-WAIT-LEAK-01: cuando shutdown dispara, wait_ccxt se cancela."""
    from core.orders.orphan_reconciler import OrphanReconciler, OrphanRecord, OrphanState

    reconciler = OrphanReconciler()

    rec = OrphanRecord(
        file=Path("/tmp/fake_intent2.json"),
        symbol="ETHUSDT",
        operation_id="op-eth",
        data={},
        state=OrphanState.DETECTED,
    )
    reconciler._records["op-eth"] = rec

    # Lanzar _reconcile_all
    task = asyncio.create_task(reconciler._reconcile_all())
    await asyncio.sleep(0.01)

    # Disparar shutdown → wait_shutdown completa, wait_ccxt debe cancelarse
    reconciler.shutdown()

    try:
        await asyncio.wait_for(task, timeout=2.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    await asyncio.sleep(0)

    all_tasks = asyncio.all_tasks()
    pending_names = {t.get_name() for t in all_tasks}
    assert "wait_ccxt" not in pending_names, (
        "wait_ccxt task sigue pendiente tras shutdown"
    )
    assert "wait_shutdown" not in pending_names, (
        "wait_shutdown task sigue pendiente tras shutdown"
    )


# ---------------------------------------------------------------------------
# scan_and_detect: tests básicos de funcionalidad
# ---------------------------------------------------------------------------


def test_scan_and_detect_empty_directory(tmp_path: Path) -> None:
    """scan_and_detect devuelve 0 cuando no hay archivos de intent."""
    from core.orders.orphan_reconciler import OrphanReconciler

    reconciler = OrphanReconciler()
    ruta_db = str(tmp_path / "db" / "orders.db")
    result = reconciler.scan_and_detect(ruta_db)

    assert result == 0
    assert not reconciler._blocked
    assert not reconciler._records


def test_scan_and_detect_finds_valid_intent(tmp_path: Path) -> None:
    """scan_and_detect detecta y bloquea símbolos con intents válidos."""
    from core.orders.orphan_reconciler import OrphanReconciler, OrphanState

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    intent = {
        "symbol": "BTCUSDT",
        "operation_id": "op-001",
        "precio": 50000.0,
        "direccion": "long",
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    (db_dir / "pre_exec_intent_op-001.json").write_text(
        json.dumps(intent), encoding="utf-8"
    )

    reconciler = OrphanReconciler()
    count = reconciler.scan_and_detect(str(db_dir / "orders.db"))

    assert count == 1
    assert "BTCUSDT" in reconciler._blocked
    assert reconciler.is_blocked("BTCUSDT")
    assert "op-001" in reconciler._records
    assert reconciler._records["op-001"].state == OrphanState.DETECTED


def test_scan_and_detect_removes_corrupt_file(tmp_path: Path) -> None:
    """scan_and_detect elimina archivos corruptos y devuelve 0."""
    from core.orders.orphan_reconciler import OrphanReconciler

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    bad_file = db_dir / "pre_exec_intent_corrupt.json"
    bad_file.write_text("{invalid json", encoding="utf-8")

    reconciler = OrphanReconciler()
    count = reconciler.scan_and_detect(str(db_dir / "orders.db"))

    assert count == 0
    assert not bad_file.exists(), "El archivo corrupto debe eliminarse"
    assert not reconciler._blocked


def test_is_blocked_returns_false_for_unknown_symbol() -> None:
    """is_blocked devuelve False para símbolos sin orphan activo."""
    from core.orders.orphan_reconciler import OrphanReconciler

    reconciler = OrphanReconciler()
    assert not reconciler.is_blocked("BTCUSDT")
    assert not reconciler.is_blocked("ETHUSDT")
