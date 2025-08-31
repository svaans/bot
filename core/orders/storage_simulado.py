from __future__ import annotations

import json
import os
import sqlite3
from typing import Dict

from core.utils.logger import configurar_logger
from .order_model import Order
from core.metrics import subscribe_simulated_order_metrics

log = configurar_logger('sim_storage', modo_silencioso=True)

STORAGE_DIR = 'storage_simulado'
STORAGE_FILE = os.path.join(STORAGE_DIR, 'ordenes.db')


def sincronizar_ordenes_simuladas(
    manager,
    persist_every: int = 5,
    archivo: str | None = None,
) -> None:
    """Carga y persiste el estado de órdenes simuladas usando SQLite."""
    bus = getattr(manager, 'bus', None)
    if not bus:
        return

    path = archivo or STORAGE_FILE
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    primera_vez = not os.path.exists(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ordenes (symbol TEXT PRIMARY KEY, data TEXT)")
    conn.commit()

    estado: Dict[str, Dict] = {}
    for symbol, data in cur.execute("SELECT symbol, data FROM ordenes"):
        rec = json.loads(data)
        estado[symbol] = rec
        manager.ordenes[symbol] = Order.from_dict(rec)

    if primera_vez:
        log.info('iniciado storage simulado')

    cambios = {'n': 0}

    def _persist() -> None:
        cur.execute('DELETE FROM ordenes')
        cur.executemany(
            'INSERT INTO ordenes(symbol, data) VALUES (?, ?)',
            [(s, json.dumps(d)) for s, d in estado.items()],
        )
        conn.commit()
        cambios['n'] = 0

    async def _on_creada(data: Dict) -> None:
        estado[data['symbol']] = data['orden']
        cambios['n'] += 1
        if cambios['n'] >= persist_every:
            _persist()

    async def _on_cerrada(data: Dict) -> None:
        estado.pop(data['symbol'], None)
        cambios['n'] += 1
        if cambios['n'] >= persist_every:
            _persist()

    bus.subscribe('orden_simulada_creada', _on_creada)
    bus.subscribe('orden_simulada_cerrada', _on_cerrada)

    subscribe_simulated_order_metrics(bus)