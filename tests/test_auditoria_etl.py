from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from core.auditoria import (
    AuditEvent,
    AuditResult,
    CURRENT_SCHEMA_VERSION,
    registrar_auditoria,
)
from core.etl import SqliteToPostgresETLConfig, sync_sqlite_auditoria_to_postgres


@pytest.mark.asyncio
async def test_sync_sqlite_auditoria_to_generic_engine(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sqlite_path = tmp_path / "auditoria.db"

    monkeypatch.setattr(
        "core.auditoria._current_utc",
        lambda: datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
    )

    registrar_auditoria(
        symbol="BTCUSDT",
        evento=AuditEvent.ENTRY,
        resultado=AuditResult.SUCCESS,
        formato="sqlite",
        archivo=str(sqlite_path),
    )

    config = SqliteToPostgresETLConfig(
        sqlite_path=sqlite_path,
        postgres_dsn=f"sqlite+pysqlite:///{tmp_path / 'dest.db'}",
        create_partitions=False,
    )
    engine = create_engine(config.postgres_dsn, future=True)

    inserted = await sync_sqlite_auditoria_to_postgres(config, engine=engine)
    assert inserted == 1

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM auditoria_historica")).scalar()
        assert count == 1
        versions = {
            row[0]
            for row in conn.execute(text("SELECT schema_version FROM auditoria_historica"))
        }
        assert versions == {CURRENT_SCHEMA_VERSION}

    monkeypatch.setattr(
        "core.auditoria._current_utc",
        lambda: datetime(2024, 1, 2, 12, tzinfo=timezone.utc),
    )

    registrar_auditoria(
        symbol="ETHUSDT",
        evento=AuditEvent.EXIT,
        resultado=AuditResult.FAILURE,
        formato="sqlite",
        archivo=str(sqlite_path),
    )

    inserted = await sync_sqlite_auditoria_to_postgres(config, engine=engine)
    assert inserted == 1

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM auditoria_historica")).scalar()
        assert count == 2


@pytest.mark.asyncio
async def test_sync_sqlite_auditoria_detects_integrity_issue(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sqlite_path = tmp_path / "auditoria.db"

    monkeypatch.setattr(
        "core.auditoria._current_utc",
        lambda: datetime(2024, 1, 3, 12, tzinfo=timezone.utc),
    )

    registrar_auditoria(
        symbol="XRPUSDT",
        evento=AuditEvent.ENTRY,
        resultado=AuditResult.SUCCESS,
        formato="sqlite",
        archivo=str(sqlite_path),
    )

    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("UPDATE auditoria SET symbol='altered'")
        conn.commit()

    config = SqliteToPostgresETLConfig(
        sqlite_path=sqlite_path,
        postgres_dsn=f"sqlite+pysqlite:///{tmp_path / 'dest.db'}",
        create_partitions=False,
    )

    with pytest.raises(RuntimeError):
        await sync_sqlite_auditoria_to_postgres(config)