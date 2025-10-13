import os
import sqlite3

import pandas as pd

from core.auditoria import registrar_auditoria


def test_registrar_auditoria_without_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivo = "auditoria_registro.csv"

    registrar_auditoria(
        symbol="BTCUSDT",
        evento="apertura",
        resultado="exitoso",
        archivo=archivo,
    )

    assert os.path.exists(archivo)
    df = pd.read_csv(archivo)
    assert df.loc[0, "symbol"] == "BTCUSDT"
    assert df.loc[0, "evento"] == "apertura"


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
        cursor = conn.execute("SELECT symbol, evento, resultado, score FROM auditoria")
        row = cursor.fetchone()

    assert row == ("ETHUSDT", "cierre", "fallido", 1.5)
