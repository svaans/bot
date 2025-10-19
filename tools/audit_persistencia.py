#!/usr/bin/env python3
"""Auditoría ligera de persistencia para capital y persistencia técnica."""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.capital_repository import CapitalRepository
from core.persistencia_tecnica import PersistenciaTecnica


def _log_operation(name: str, action: Callable[[], Any], *, transform: Callable[[Any], Any] | None = None) -> Any:
    start = time.perf_counter()
    error: str | None = None
    result_payload: Any | None = None
    try:
        result = action()
        if transform is not None:
            result_payload = transform(result)
    except Exception as exc:  # pragma: no cover - herramienta de diagnóstico
        error = repr(exc)
        result = None
    duration = (time.perf_counter() - start) * 1000
    payload = {
        "operation": name,
        "duration_ms": round(duration, 3),
        "error": error,
    }
    if result_payload is not None:
        payload["result"] = result_payload
    print(json.dumps(payload, ensure_ascii=False))
    return result


def _audit_capital(path: Path) -> None:
    repo = CapitalRepository(path=path)

    def snapshot_payload(snapshot: Any) -> dict[str, Any]:
        return {
            "capital_por_simbolo": dict(snapshot.capital_por_simbolo),
            "disponible_global": snapshot.disponible_global,
        }

    _log_operation("capital.load_missing", repo.load, transform=snapshot_payload)
    _log_operation(
        "capital.save_initial",
        lambda: repo.save({"BTC/USDT": 150.5, "ETH/USDT": 0.0}, 250.0),
    )
    _log_operation("capital.load_after_save", repo.load, transform=snapshot_payload)

    def concurrent_saves() -> dict[str, Any]:
        with ThreadPoolExecutor(max_workers=4) as executor:
            for idx in range(10):
                executor.submit(
                    repo.save,
                    {"BTC/USDT": 100.0 + idx, "ETH/USDT": 50.0 + idx},
                    200.0 + idx,
                )
        return snapshot_payload(repo.load())

    _log_operation("capital.concurrent_saves", concurrent_saves, transform=lambda x: x)

    def read_json() -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    _log_operation("capital.read_raw", read_json)


def _audit_persistencia_tecnica() -> None:
    persistencia = PersistenciaTecnica(minimo=2)

    def export_payload() -> dict[str, Any]:
        persistencia.actualizar("BTC/USDT", {"mom": True, "adx": False})
        persistencia.actualizar("BTC/USDT", {"mom": True, "adx": True})
        return persistencia.export_state()

    snapshot = _log_operation("persistencia.export", export_payload, transform=lambda x: x)

    restored = PersistenciaTecnica()
    if isinstance(snapshot, dict):
        _log_operation("persistencia.load", lambda: restored.load_state(snapshot))

    def concurrent_updates() -> dict[str, Any]:
        with ThreadPoolExecutor(max_workers=4) as executor:
            for _ in range(20):
                executor.submit(
                    persistencia.filtrar_persistentes,
                    "BTC/USDT",
                    {"mom": True, "adx": False},
                )
        return persistencia.export_state()["conteo"].get("BTC/USDT", {})

    _log_operation("persistencia.concurrent_updates", concurrent_updates, transform=lambda x: x)


def main() -> None:
    with TemporaryDirectory() as tmp:
        capital_path = Path(tmp) / "capital.json"
        _audit_capital(capital_path)
    _audit_persistencia_tecnica()


if __name__ == "__main__":
    main()