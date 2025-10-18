"""Sincronización de auditoría SQLite hacia PostgreSQL particionado."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from core.auditoria import AUDIT_COLUMNS, verificar_integridad_sqlite

log = logging.getLogger(__name__)

_POSTGRES_TYPES = {
    "timestamp": "TIMESTAMPTZ NOT NULL",
    "schema_version": "TEXT NOT NULL",
    "operation_id": "TEXT NOT NULL",
    "order_id": "TEXT",
    "symbol": "TEXT NOT NULL",
    "evento": "TEXT NOT NULL",
    "resultado": "TEXT NOT NULL",
    "source": "TEXT",
    "estrategias_activas": "JSONB",
    "score": "DOUBLE PRECISION",
    "rsi": "DOUBLE PRECISION",
    "volumen_relativo": "DOUBLE PRECISION",
    "tendencia": "TEXT",
    "razon": "TEXT",
    "capital_actual": "DOUBLE PRECISION",
    "config_usada": "JSONB",
    "comentario": "TEXT",
}

_GENERIC_TYPES = {
    "timestamp": "TEXT NOT NULL",
    "schema_version": "TEXT NOT NULL",
    "operation_id": "TEXT NOT NULL",
    "order_id": "TEXT",
    "symbol": "TEXT NOT NULL",
    "evento": "TEXT NOT NULL",
    "resultado": "TEXT NOT NULL",
    "source": "TEXT",
    "estrategias_activas": "TEXT",
    "score": "REAL",
    "rsi": "REAL",
    "volumen_relativo": "REAL",
    "tendencia": "TEXT",
    "razon": "TEXT",
    "capital_actual": "REAL",
    "config_usada": "TEXT",
    "comentario": "TEXT",
}

_JSON_COLUMNS = {"estrategias_activas", "config_usada"}


@dataclass(slots=True)
class SqliteToPostgresETLConfig:
    """Configuración del proceso ETL para auditorías."""

    sqlite_path: Path
    postgres_dsn: str
    target_table: str = "auditoria_historica"
    schema: str | None = None
    chunk_size: int = 1_000
    verify_integrity: bool = True
    create_partitions: bool = True

    def qualified_table(self) -> str:
        """Nombre de tabla calificado incluyendo esquema si aplica."""

        table = _validate_identifier(self.target_table)
        if self.schema:
            schema = _validate_identifier(self.schema)
            return f"{schema}.{table}"
        return table


async def sync_sqlite_auditoria_to_postgres(
    config: SqliteToPostgresETLConfig,
    *,
    engine: Engine | None = None,
) -> int:
    """Sincroniza registros de auditoría de SQLite hacia la base destino.

    Devuelve la cantidad de filas insertadas. El proceso valida la integridad
    mediante ``verificar_integridad_sqlite`` si ``config.verify_integrity`` es
    ``True``.
    """

    if config.verify_integrity:
        integridad_ok = await asyncio.to_thread(
            verificar_integridad_sqlite, config.sqlite_path
        )
        if not integridad_ok:
            raise RuntimeError(
                "La verificación de integridad falló, abortando sincronización"
            )
    if engine is None:
        engine = create_engine(config.postgres_dsn, future=True)
    inserted = 0
    last_timestamp: datetime | None
    with engine.begin() as conn:
        _ensure_target_schema(conn, config)
        last_timestamp = _fetch_last_timestamp(conn, config)
    insert_statement = _build_insert_statement(config)

    def _run() -> int:
        nonlocal last_timestamp, inserted
        with closing(sqlite3.connect(config.sqlite_path)) as sqlite_conn:
            sqlite_conn.row_factory = sqlite3.Row
            cursor = sqlite_conn.execute(
                f"SELECT {', '.join(AUDIT_COLUMNS)} FROM auditoria ORDER BY timestamp ASC"
            )
            while True:
                rows = cursor.fetchmany(config.chunk_size)
                if not rows:
                    break
                prepared = _prepare_rows(rows, last_timestamp)
                if not prepared:
                    continue
                timestamps = [row["timestamp"] for row in prepared]
                with engine.begin() as target_conn:
                    dialect_name = target_conn.dialect.name
                    if config.create_partitions and dialect_name == "postgresql":
                        _ensure_partitions(target_conn, timestamps, config)
                    payload = []
                    for record in prepared:
                        normalized = dict(record)
                        if dialect_name == "postgresql":
                            for json_column in _JSON_COLUMNS:
                                value = normalized.get(json_column)
                                if isinstance(value, str):
                                    try:
                                        normalized[json_column] = json.loads(value)
                                    except json.JSONDecodeError:
                                        log.debug(
                                            "No se pudo decodificar JSON en columna %s, se conserva como texto",
                                            json_column,
                                        )
                        else:
                            normalized["timestamp"] = normalized["timestamp"].isoformat()
                        payload.append(normalized)
                    target_conn.execute(insert_statement, payload)
                last_timestamp = max(timestamps)
                inserted += len(prepared)
        return inserted

    return await asyncio.to_thread(_run)


def _ensure_target_schema(conn: Connection, config: SqliteToPostgresETLConfig) -> None:
    table = config.qualified_table()
    if conn.dialect.name == "postgresql":
        if config.schema:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.schema}"))
        columns = ",\n    ".join(
            f"{column} {_POSTGRES_TYPES[column]}" for column in AUDIT_COLUMNS
        )
        conn.execute(
            text(
                f"""
CREATE TABLE IF NOT EXISTS {table} (
    {columns}
) PARTITION BY RANGE (timestamp);
"""
            )
        )
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{config.target_table}_natural_key "
                f"ON {table} (timestamp, operation_id, evento)"
            )
        )
        return
    columns = ", ".join(
        f"{column} {_GENERIC_TYPES[column]}" for column in AUDIT_COLUMNS
    )
    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {table} ({columns})"
        )
    )
    conn.execute(
        text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{config.target_table}_natural_key "
            f"ON {table} (timestamp, operation_id, evento)"
        )
    )


def _fetch_last_timestamp(
    conn: Connection, config: SqliteToPostgresETLConfig
) -> datetime | None:
    result = conn.execute(
        text(f"SELECT MAX(timestamp) FROM {config.qualified_table()}")
    ).scalar()
    if result is None:
        return None
    if isinstance(result, datetime):
        return result
    return datetime.fromisoformat(str(result))


def _build_insert_statement(config: SqliteToPostgresETLConfig):
    placeholders = ", ".join(f":{column}" for column in AUDIT_COLUMNS)
    columns = ", ".join(AUDIT_COLUMNS)
    table = config.qualified_table()
    return text(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    )


def _prepare_rows(
    rows: Sequence[sqlite3.Row], last_timestamp: datetime | None
) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for row in rows:
        registro = {column: row[column] for column in AUDIT_COLUMNS}
        timestamp = datetime.fromisoformat(registro["timestamp"]).astimezone(timezone.utc)
        if last_timestamp and timestamp <= last_timestamp:
            continue
        registro["timestamp"] = timestamp
        prepared.append(registro)
    return prepared


def _ensure_partitions(
    conn: Connection, timestamps: Iterable[datetime], config: SqliteToPostgresETLConfig
) -> None:
    for month_start in {_month_start(ts) for ts in timestamps}:
        partition = f"{config.target_table}_{month_start:%Y%m}"
        full_partition = (
            f"{config.schema}.{partition}" if config.schema else partition
        )
        start_iso = month_start.isoformat()
        end_iso = (_next_month(month_start)).isoformat()
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {full_partition} PARTITION OF {config.qualified_table()} "
                f"FOR VALUES FROM ('{start_iso}') TO ('{end_iso}')"
            )
        )


def _month_start(timestamp: datetime) -> datetime:
    ts = timestamp.astimezone(timezone.utc)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(timestamp: datetime) -> datetime:
    return timestamp + relativedelta(months=1)


def _validate_identifier(name: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    if not name or any(char not in allowed for char in name):
        raise ValueError(f"Identificador SQL inválido: {name}")
    return name