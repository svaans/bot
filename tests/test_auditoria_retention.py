import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.auditoria_retention import archive_old_audit_files, schedule_weekly_archive


class DummyStorageBackend:
    def __init__(self) -> None:
        self.uploads: list[tuple[Path, str]] = []

    async def upload_file(self, local_path: Path, remote_path: str) -> None:
        self.uploads.append((local_path, remote_path))


@pytest.mark.asyncio
async def test_archive_old_audit_files_moves_and_purges(tmp_path):
    base_dir = tmp_path / "informes"
    viejo = base_dir / "20240101"
    viejo.mkdir(parents=True)
    viejo_archivo = viejo / "auditoria_20240101.jsonl.gz"
    viejo_archivo.write_bytes(b"contenido")
    antiguo_mtime = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
    os.utime(viejo_archivo, (antiguo_mtime, antiguo_mtime))

    reciente = base_dir / "20240105"
    reciente.mkdir(parents=True)
    reciente_archivo = reciente / "auditoria_20240105.jsonl.gz"
    reciente_archivo.write_bytes(b"nuevo")

    backend = DummyStorageBackend()
    archivados = await archive_old_audit_files(
        base_dir=base_dir,
        storage=backend,
        days_threshold=7,
        remote_prefix="auditoria",
        delete_local=True,
    )

    assert archivados == [viejo_archivo]
    assert backend.uploads == [
        (viejo_archivo, "auditoria/20240101/auditoria_20240101.jsonl.gz")
    ]
    assert not viejo_archivo.exists()
    assert reciente_archivo.exists()


@pytest.mark.asyncio
async def test_schedule_weekly_archive_exits_with_stop_event(tmp_path):
    backend = DummyStorageBackend()
    stop_event = asyncio.Event()
    stop_event.set()

    await schedule_weekly_archive(
        base_dir=tmp_path,
        storage=backend,
        days_threshold=7,
        remote_prefix="auditoria",
        stop_event=stop_event,
    )