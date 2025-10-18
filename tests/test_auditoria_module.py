import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.auditoria import (
    AuditEvent,
    AuditResult,
    CURRENT_SCHEMA_VERSION,
    registrar_auditoria,
    verificar_integridad_sqlite,
)
from observability.metrics import (
    AUDITORIA_ERRORS_TOTAL,
    AUDITORIA_LOCK_CONTENTION_TOTAL,
    AUDITORIA_LOCK_WAIT_SECONDS,
    AUDITORIA_WRITE_LATENCY_SECONDS,
    AUDITORIA_WRITES_TOTAL,
)


def test_registrar_auditoria_without_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivo = "auditoria_registro.jsonl"

    registrar_auditoria(
        symbol="BTCUSDT",
        evento="apertura",
        resultado="exitoso",
        archivo=archivo,
    )

    assert os.path.exists(archivo)
    df = pd.read_json(archivo, lines=True)
    assert df.loc[0, "symbol"] == "BTCUSDT"
    assert df.loc[0, "evento"] == AuditEvent.ENTRY.value
    assert df.loc[0, "resultado"] == AuditResult.SUCCESS.value
    assert df.loc[0, "source"] == "unknown"
    assert str(df.loc[0, "schema_version"]) == CURRENT_SCHEMA_VERSION
    operation_id = df.loc[0, "operation_id"]
    uuid.UUID(operation_id)


def test_registrar_auditoria_sqlite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    archivo = "auditoria.db"

    registrar_auditoria(
        symbol="ETHUSDT",
        evento="cierre",
        resultado="fallido",
        archivo=archivo,
        formato="sqlite",
        score=1.5,
    )

    assert os.path.exists(archivo)
    with sqlite3.connect(archivo) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT symbol, evento, resultado, score, operation_id, source, schema_version "
            "FROM auditoria"
        )
        row = cursor.fetchone()

    assert row["symbol"] == "ETHUSDT"
    assert row["evento"] == AuditEvent.EXIT.value
    assert row["resultado"] == AuditResult.FAILURE.value
    assert row["score"] == 1.5
    uuid.UUID(row["operation_id"])
    assert row["source"] == "unknown"
    assert row["schema_version"] == CURRENT_SCHEMA_VERSION
    checksum_row = conn.execute(
        "SELECT total_registros FROM auditoria_checksums"
    ).fetchone()
    assert checksum_row[0] == 1


def test_registrar_auditoria_daily_rotation(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    primera_fecha = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    monkeypatch.setattr("core.auditoria._current_utc", lambda: primera_fecha)

    registrar_auditoria(
        symbol="BNBUSDT",
        evento=AuditEvent.ENTRY,
        resultado=AuditResult.SUCCESS,
    )

    carpeta_esperada = Path("informes") / "20240101"
    archivo_esperado = carpeta_esperada / "auditoria_20240101.jsonl"
    assert archivo_esperado.exists()

    segunda_fecha = datetime(2024, 1, 2, 6, tzinfo=timezone.utc)
    monkeypatch.setattr("core.auditoria._current_utc", lambda: segunda_fecha)

    registrar_auditoria(
        symbol="BNBUSDT",
        evento=AuditEvent.EXIT,
        resultado=AuditResult.SUCCESS,
    )

    assert not archivo_esperado.exists()
    archivo_comprimido = archivo_esperado.with_suffix(".jsonl.gz")
    assert archivo_comprimido.exists()
    nuevo_archivo = Path("informes") / "20240102" / "auditoria_20240102.jsonl"
    assert nuevo_archivo.exists()


def test_verificar_integridad_sqlite_detecta_manipulacion(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivo = Path("auditoria.db")

    registrar_auditoria(
        symbol="SOLUSDT",
        evento="entrada",
        resultado="exitoso",
        archivo=str(archivo),
        formato="sqlite",
    )

    assert verificar_integridad_sqlite(archivo)

    with sqlite3.connect(archivo) as conn:
        conn.execute("UPDATE auditoria SET symbol = 'ALTERED'")
        conn.commit()

    assert not verificar_integridad_sqlite(archivo)


def _counter_value(metric, formato: str) -> float:
    return metric.labels(formato=formato)._value


def _histogram_count(metric, formato: str) -> int:
    return len(metric.labels(formato=formato)._observations)


def test_auditoria_metrics_registran_escritura(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    formato = "jsonl"
    inicial_writes = _counter_value(AUDITORIA_WRITES_TOTAL, formato)
    inicial_errors = _counter_value(AUDITORIA_ERRORS_TOTAL, formato)
    inicial_latency = _histogram_count(AUDITORIA_WRITE_LATENCY_SECONDS, formato)
    inicial_wait = _histogram_count(AUDITORIA_LOCK_WAIT_SECONDS, formato)
    inicial_contention = _counter_value(AUDITORIA_LOCK_CONTENTION_TOTAL, formato)

    registrar_auditoria(
        symbol="XRPUSDT",
        evento=AuditEvent.ENTRY,
        resultado=AuditResult.SUCCESS,
    )

    assert _counter_value(AUDITORIA_WRITES_TOTAL, formato) == inicial_writes + 1
    assert _counter_value(AUDITORIA_ERRORS_TOTAL, formato) == inicial_errors
    assert _histogram_count(AUDITORIA_WRITE_LATENCY_SECONDS, formato) == inicial_latency + 1
    assert _histogram_count(AUDITORIA_LOCK_WAIT_SECONDS, formato) == inicial_wait + 1
    assert _counter_value(AUDITORIA_LOCK_CONTENTION_TOTAL, formato) >= inicial_contention


def test_auditoria_metrics_registran_errores(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    formato = "jsonl"
    inicial_writes = _counter_value(AUDITORIA_WRITES_TOTAL, formato)
    inicial_errors = _counter_value(AUDITORIA_ERRORS_TOTAL, formato)
    inicial_latency = _histogram_count(AUDITORIA_WRITE_LATENCY_SECONDS, formato)

    def _boom(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr("core.auditoria._append_jsonl", _boom)

    with pytest.raises(OSError):
        registrar_auditoria(
            symbol="ADAUSDT",
            evento=AuditEvent.EXIT,
            resultado=AuditResult.FAILURE,
        )

    assert _counter_value(AUDITORIA_WRITES_TOTAL, formato) == inicial_writes
    assert _counter_value(AUDITORIA_ERRORS_TOTAL, formato) == inicial_errors + 1
    assert _histogram_count(AUDITORIA_WRITE_LATENCY_SECONDS, formato) == inicial_latency
