import os
import sqlite3
import pandas as pd
from core.orders import real_orders


def test_persist_multiple_operations(tmp_path, monkeypatch):
    ops = [
        {
            "symbol": "AAA/USDT",
            "precio_entrada": 1.0,
            "cantidad": 1.0,
            "stop_loss": 0.9,
            "take_profit": 1.1,
            "timestamp": "t1",
            "estrategias_activas": "",
            "tendencia": "",
            "max_price": 0.0,
            "direccion": "long",
            "precio_cierre": 1.0,
            "fecha_cierre": "",
            "motivo_cierre": "",
            "retorno_total": 0.0,
        },
        {
            "symbol": "AAA/USDT",
            "precio_entrada": 2.0,
            "cantidad": 2.0,
            "stop_loss": 1.8,
            "take_profit": 2.2,
            "timestamp": "t2",
            "estrategias_activas": "",
            "tendencia": "",
            "max_price": 0.0,
            "direccion": "long",
            "precio_cierre": 2.0,
            "fecha_cierre": "",
            "motivo_cierre": "",
            "retorno_total": 0.0,
        },
    ]
    db = tmp_path / "ops.db"
    work = tmp_path / "work"
    work.mkdir()
    real_orders.RUTA_DB = str(db)
    with monkeypatch.context() as m:
        m.chdir(work)
        real_orders._persist_operations(ops)

    rows = sqlite3.connect(db).execute("SELECT COUNT(*) FROM operaciones").fetchone()[0]
    df = pd.read_parquet(work / "ordenes_reales" / "aaa_usdt.parquet")
    assert rows == 2
    assert len(df) == 2