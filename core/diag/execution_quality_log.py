"""Registro append-only de fills reales para análisis de slippage (Fase 6).

Cada línea es un JSON con precio de referencia (ticker), fill, slippage vs ticker,
y opcionalmente precio de señal del bot y slippage vs esa señal.

Variables de entorno:

- ``EXECUTION_QUALITY_LOG_ENABLED`` (default ``1``): ``0``/``false`` desactiva escrituras.
- ``EXECUTION_QUALITY_LOG_PATH``: ruta del ``.jsonl``; por defecto ``logs/ejecuciones.jsonl``
  bajo la raíz del repo (:func:`core.repo_paths.repo_root`).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from core.repo_paths import repo_root, resolve_under_repo


def execution_quality_log_enabled() -> bool:
    v = os.getenv("EXECUTION_QUALITY_LOG_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def execution_quality_log_path() -> Path:
    raw = os.getenv("EXECUTION_QUALITY_LOG_PATH", "").strip()
    if raw:
        return resolve_under_repo(raw)
    return (repo_root() / "logs" / "ejecuciones.jsonl").resolve()


def append_ejecucion_mercado(record: dict[str, Any]) -> None:
    """Añade un registro al JSONL (no falla el trading si el disco falla)."""
    if not execution_quality_log_enabled():
        return
    path = execution_quality_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass


def load_ejecuciones_jsonl(path: Path | None = None) -> list[dict[str, Any]]:
    """Carga todas las líneas JSON válidas; ignora líneas vacías o corruptas."""

    p = path or execution_quality_log_path()
    if not p.is_file():
        return []
    out: list[dict[str, Any]] = []
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                out.append(row)
    return out
