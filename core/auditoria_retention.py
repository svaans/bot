"""Utilidades para rotación y retención prolongada de auditorías."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)

UTC = timezone.utc


class StorageBackend(Protocol):
    """Contrato mínimo para backends de almacenamiento externo."""

    async def upload_file(self, local_path: Path, remote_path: str) -> None:
        """Sube ``local_path`` a ``remote_path``."""


@dataclass(slots=True)
class S3CompatibleStorageBackend:
    """Backend compatible con S3 (AWS, Backblaze, MinIO, etc.)."""

    bucket: str
    region_name: str | None = None
    endpoint_url: str | None = None
    profile_name: str | None = None

    async def upload_file(self, local_path: Path, remote_path: str) -> None:
        """Sube ``local_path`` a ``remote_path`` utilizando boto3."""

        def _upload() -> None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - configuración de entorno
                raise RuntimeError(
                    "boto3 es requerido para usar S3CompatibleStorageBackend"
                ) from exc

            session = boto3.session.Session(profile_name=self.profile_name)
            client = session.client(
                "s3",
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
            )
            client.upload_file(str(local_path), self.bucket, remote_path)

        await asyncio.to_thread(_upload)


async def archive_old_audit_files(
    *,
    base_dir: Path,
    storage: StorageBackend,
    days_threshold: int,
    remote_prefix: str = "auditoria",
    delete_local: bool = True,
) -> list[Path]:
    """Sube auditorías antiguas al backend y purga copias locales."""

    now = datetime.now(UTC)
    cutoff = now - timedelta(days=days_threshold)
    archived: list[Path] = []
    if not base_dir.exists():
        log.debug("Directorio de auditorías %s no existe, nada que archivar", base_dir)
        return archived
    for file_path in base_dir.rglob("*"):
        if not file_path.is_file():
            continue
        if not _es_archivo_auditoria(file_path):
            continue
        if datetime.fromtimestamp(file_path.stat().st_mtime, UTC) > cutoff:
            continue
        remote_path = _build_remote_path(file_path, base_dir, remote_prefix)
        await storage.upload_file(file_path, remote_path)
        archived.append(file_path)
        if delete_local:
            try:
                file_path.unlink()
            except OSError as error:
                log.warning("No se pudo eliminar %s tras archivarlo: %s", file_path, error)
    return archived


async def schedule_weekly_archive(
    *,
    base_dir: Path,
    storage: StorageBackend,
    days_threshold: int,
    remote_prefix: str = "auditoria",
    stop_event: asyncio.Event | None = None,
) -> None:
    """Ejecuta ``archive_old_audit_files`` cada 7 días hasta que ``stop_event`` se active."""

    if stop_event is None:
        stop_event = asyncio.Event()
    interval = timedelta(days=7)
    while not stop_event.is_set():
        await archive_old_audit_files(
            base_dir=base_dir,
            storage=storage,
            days_threshold=days_threshold,
            remote_prefix=remote_prefix,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval.total_seconds())
        except asyncio.TimeoutError:
            continue


def _es_archivo_auditoria(file_path: Path) -> bool:
    nombre = file_path.name
    if not nombre.startswith("auditoria_"):
        return False
    sufijos = file_path.suffixes
    return any(
        sufijos == ext
        for ext in (
            [".jsonl"],
            [".jsonl", ".gz"],
            [".csv"],
            [".csv", ".gz"],
        )
    )


def _build_remote_path(file_path: Path, base_dir: Path, remote_prefix: str) -> str:
    try:
        relative = file_path.relative_to(base_dir)
    except ValueError:
        relative = file_path.name
    else:
        relative = relative.as_posix()
    if isinstance(relative, Path):
        relative = relative.as_posix()
    prefix = remote_prefix.strip("/")
    if not prefix:
        return str(relative)
    return f"{prefix}/{relative}"