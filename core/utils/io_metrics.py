"""Helpers para instrumentar escrituras a disco y generación de reportes."""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Callable, Iterator, TypeVar

from observability.metrics import (
    METRIC_WRITE_LATENCY_SECONDS,
    REPORT_GENERATION_DURATION_SECONDS,
)

__all__ = [
    "observe_disk_write",
    "report_generation_timer",
]


_T = TypeVar("_T")


def _extension_from_path(path: str | os.PathLike[str]) -> str:
    ext = os.path.splitext(str(path))[1].lower()
    return ext if ext else "unknown"


def observe_disk_write(
    operation: str,
    path: str | os.PathLike[str],
    writer: Callable[[], _T],
) -> _T:
    """Ejecuta ``writer`` registrando la latencia de escritura asociada."""

    inicio = time.monotonic_ns()
    try:
        return writer()
    finally:
        duracion_ns = max(time.monotonic_ns() - inicio, 0)
        METRIC_WRITE_LATENCY_SECONDS.labels(
            operation=operation,
            extension=_extension_from_path(path),
        ).observe(duracion_ns / 1_000_000_000)


@contextmanager
def report_generation_timer() -> Iterator[Callable[[str], None]]:
    """Context manager que reporta la duración de generación de informes."""

    inicio = time.monotonic_ns()
    estado = "success"

    def set_status(value: str) -> None:
        nonlocal estado
        estado = value

    try:
        yield set_status
    except Exception:
        estado = "error"
        raise
    finally:
        duracion_ns = max(time.monotonic_ns() - inicio, 0)
        REPORT_GENERATION_DURATION_SECONDS.labels(status=estado).observe(
            duracion_ns / 1_000_000_000
        )