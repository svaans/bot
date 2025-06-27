import os
import threading
import asyncio
import sqlite3
from aiohttp import web
import pandas as pd
import pytest

from core.orders import real_orders
from core.utils import utils
from core.workers import order_worker_client


def make_guardar(tmpdir):
    def _guardar(symbol, orden):
        path = tmpdir / f"{symbol.replace('/', '_').lower()}.parquet"
        os.makedirs(str(tmpdir), exist_ok=True)
        df = pd.DataFrame([orden])
        if path.exists():
            prev = pd.read_parquet(path)
            df = pd.concat([prev, df], ignore_index=True)
        df.to_parquet(path, index=False)
    return _guardar


def start_fake_worker(tmpdir, db_path, port):
    async def handle(request):
        ops = await request.json()
        real_orders.RUTA_DB = str(db_path)
        patch = make_guardar(tmpdir)
        utils.guardar_orden_real = patch
        real_orders.guardar_orden_real = patch
        real_orders._persist_operations(ops)
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_post('/orders', handle)
    runner = web.AppRunner(app)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ready = threading.Event()
    async def run():
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', port)
        await site.start()
        ready.set()
        await stop.wait()
        await runner.cleanup()
    stop = asyncio.Event()
    thread = threading.Thread(target=loop.run_until_complete, args=(run(),))
    thread.start()
    return stop, thread, ready


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
    patch_func = make_guardar(old_dir)
    utils.guardar_orden_real = patch_func
    real_orders.guardar_orden_real = patch_func
    real_orders.RUTA_DB = str(old_db)
    real_orders._persist_operations([op])

    row_old = sqlite3.connect(old_db).execute('SELECT symbol FROM operaciones').fetchall()
    df_old = pd.read_parquet(old_dir / 'aaa_usdt.parquet')

    new_db = tmp_path / 'new.db'
    new_dir = tmp_path / 'new'
    port = 9102
    stop, thread, ready = start_fake_worker(new_dir, new_db, port)
    ready.wait()
    url = f'http://localhost:{port}/orders'
    os.environ['ORDER_WORKER_URL'] = url
    order_worker_client.WORKER_URL = url

    patch_new = make_guardar(new_dir)
    utils.guardar_orden_real = patch_new
    real_orders.guardar_orden_real = patch_new
    real_orders.RUTA_DB = str(new_db)

    real_orders._BUFFER_OPERACIONES.clear()
    real_orders._BUFFER_OPERACIONES.append(op)
    real_orders.flush_operaciones()
    order_worker_client.wait_pending()

    stop.set()
    thread.join()

    row_new = sqlite3.connect(new_db).execute('SELECT symbol FROM operaciones').fetchall()
    df_new = pd.read_parquet(new_dir / 'aaa_usdt.parquet')

    assert row_old == row_new
    pd.testing.assert_frame_equal(df_old, df_new)