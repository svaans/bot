import os
import sqlite3
import uuid

import pandas as pd

from core.auditoria import AuditEvent, AuditResult, registrar_auditoria


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
        cursor = conn.execute(
            "SELECT symbol, evento, resultado, score, operation_id, source FROM auditoria"
        )
        row = cursor.fetchone()

    assert row[0:4] == ("ETHUSDT", AuditEvent.EXIT.value, AuditResult.FAILURE.value, 1.5)
    uuid.UUID(row[4])
    assert row[5] == "unknown"
