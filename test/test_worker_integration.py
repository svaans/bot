import os
import threading
import sqlite3
import json
from concurrent import futures
import grpc
import pandas as pd
import pytest

from core.orders import real_orders
from core.workers import order_worker_client
from core import orders_pb2, orders_pb2_grpc



def start_fake_worker(tmpdir, db_path, port):
    real_orders.RUTA_DB = str(db_path)
    os.makedirs(tmpdir, exist_ok=True)

    class Servicer(orders_pb2_grpc.OrderWriterServicer):
        def WriteOrders(self, request, context):
            ops = []
            for o in request.orders:
                if o.estrategias_activas:
                    estrategias = json.loads(o.estrategias_activas)
                else:
                    estrategias = ""
                ops.append(
                    {
                        "symbol": o.symbol,
                        "precio_entrada": o.precio_entrada,
                        "cantidad": o.cantidad,
                        "stop_loss": o.stop_loss,
                        "take_profit": o.take_profit,
                        "timestamp": o.timestamp,
                        "estrategias_activas": estrategias,
                        "tendencia": o.tendencia,
                        "max_price": o.max_price,
                        "direccion": o.direccion,
                        "precio_cierre": o.precio_cierre,
                        "fecha_cierre": o.fecha_cierre,
                        "motivo_cierre": o.motivo_cierre,
                        "retorno_total": o.retorno_total,
                    }
                )
            cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                real_orders._persist_operations(ops)
            finally:
                os.chdir(cwd)
            return orders_pb2.WriteResponse(ok=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    orders_pb2_grpc.add_OrderWriterServicer_to_server(Servicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    ready = threading.Event()
    def run():
        ready.set()
        server.wait_for_termination()

    thread = threading.Thread(target=run)
    thread.start()
    return server, thread, ready


def test_worker_persistence(tmp_path, monkeypatch):
    op = {
        "symbol": "AAA/USDT",
        "precio_entrada": 1.0,
        "cantidad": 1.0,
        "stop_loss": 0.8,
        "take_profit": 1.2,
        "timestamp": "2024-01-01T00:00:00",
        "estrategias_activas": {"e": 1},
        "tendencia": "bull",
        "max_price": 1.1,
        "direccion": "long",
        "precio_cierre": 1.05,
        "fecha_cierre": "2024-01-01T01:00:00",
        "motivo_cierre": "tp",
        "retorno_total": 0.05,
    }

    old_db = tmp_path / 'old.db'
    old_dir = tmp_path / 'old'
    os.makedirs(old_dir, exist_ok=True)
    real_orders.RUTA_DB = str(old_db)
    with monkeypatch.context() as m:
        m.chdir(old_dir)
        real_orders._persist_operations([op])

    row_old = sqlite3.connect(old_db).execute('SELECT symbol FROM operaciones').fetchall()
    df_old = pd.read_parquet(old_dir / 'ordenes_reales' / 'aaa_usdt.parquet')

    new_db = tmp_path / 'new.db'
    new_dir = tmp_path / 'new'
    port = 9102
    server, thread, ready = start_fake_worker(new_dir, new_db, port)
    ready.wait()
    os.environ['ORDERS_WORKER_HOST'] = 'localhost'
    os.environ['ORDERS_WORKER_PORT'] = str(port)
    order_worker_client.WORKER_HOST = 'localhost'
    order_worker_client.WORKER_PORT = port
    order_worker_client._ADDRESS = f'localhost:{port}'
    order_worker_client._channel = grpc.insecure_channel(order_worker_client._ADDRESS)
    order_worker_client._stub = orders_pb2_grpc.OrderWriterStub(order_worker_client._channel)

    real_orders.RUTA_DB = str(new_db)

    with monkeypatch.context() as m2:
        m2.chdir(new_dir)
        real_orders._BUFFER_OPERACIONES.clear()
        real_orders._BUFFER_OPERACIONES.append(op)
        real_orders.flush_operaciones()
        order_worker_client.wait_pending()

    server.stop(None)
    thread.join()

    row_new = sqlite3.connect(new_db).execute('SELECT symbol FROM operaciones').fetchall()
    df_new = pd.read_parquet(new_dir / 'ordenes_reales' / 'aaa_usdt.parquet')

    assert row_old == row_new
    pd.testing.assert_frame_equal(df_old, df_new)