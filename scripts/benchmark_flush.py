import os
import sys
import time
import shutil
from typing import Dict, Any

# Ensure scripts can import project modules
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Ensure tests run in isolated files
os.environ['ORDENES_DB_PATH'] = 'ordenes_reales/bench_db.sqlite'

from core.orders import real_orders
import logging
import atexit

for name in ('trader', 'ordenes'):
    logging.getLogger(name).setLevel(logging.WARNING)

atexit.unregister(real_orders.flush_operaciones)

# Evita escrituras en Parquet durante el benchmark
real_orders.guardar_orden_real = lambda *args, **kwargs: None

N_OPERACIONES = 500
BATCH_SIZES = [50, 100, 200]


def crear_operacion(i: int) -> Dict[str, Any]:
    """Genera una operación de prueba."""
    ts = time.time()
    return {
        'symbol': 'BTC/USDT',
        'precio_entrada': 100.0 + i,
        'cantidad': 0.01,
        'stop_loss': 90.0,
        'take_profit': 110.0,
        'timestamp': ts,
        'estrategias_activas': {'demo': True},
        'tendencia': 'alcista',
        'max_price': 100.0,
        'direccion': 'long',
        'precio_cierre': 105.0,
        'fecha_cierre': ts,
        'motivo_cierre': 'tp',
        'retorno_total': 0.05,
    }


def preparar_entorno():
    real_orders._BUFFER_OPERACIONES.clear()
    shutil.rmtree('ordenes_reales', ignore_errors=True)
    if os.path.exists(real_orders.RUTA_DB):
        os.remove(real_orders.RUTA_DB)


def ejecutar_prueba(batch: int, use_pool: bool) -> float:
    preparar_entorno()
    real_orders._USE_PROCESS_POOL = use_pool
    real_orders._FLUSH_BATCH_SIZE = batch
    real_orders._BUFFER_OPERACIONES.extend(
        crear_operacion(i) for i in range(N_OPERACIONES)
    )
    inicio = time.time()
    real_orders.flush_operaciones()
    return time.time() - inicio


def main():
    for use_pool in (False, True):
        for batch in BATCH_SIZES:
            duracion = ejecutar_prueba(batch, use_pool)
            print(
                f"use_pool={use_pool} batch={batch} -> {duracion:.2f}s"
            )


if __name__ == '__main__':
    main()
