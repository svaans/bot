import os

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
