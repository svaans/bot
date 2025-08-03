"""Configuración compartida para las pruebas.

Este módulo ajusta ``sys.path`` para que los tests puedan importar el paquete
``core`` directamente y además se encarga de limpiar el estado global que el
bot mantiene entre ejecuciones. Varias partes del proyecto (por ejemplo
``core.supervisor`` y ``core.orders.real_orders``) utilizan caches y
diccionarios globales, lo que puede provocar interferencias cuando los tests se
ejecutan de forma secuencial en un mismo proceso.  Para asegurar aislamiento se
restablecen dichas estructuras tras cada prueba.
"""

import sys
from pathlib import Path
import asyncio

import pytest

# Permite importar el paquete principal sin instalarlo
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

@pytest.fixture(autouse=True)
async def limpiar_estado_global() -> None:
    """Reinicia caches y tareas globales después de cada test.

    Muchos componentes del bot asumen una única instancia por proceso y usan
    variables globales para su funcionamiento.  Para evitar que una prueba
    afecte a otra, esta fixture cancela las tareas supervisadas y limpia los
    buffers de órdenes y operaciones.  Se ejecuta automáticamente en cada test
    para garantizar un entorno limpio.
    """

    # ---- Aquí se ejecuta el test ----
    yield

    # Limpieza de tareas supervisadas
    from core.supervisor import (
        tasks,
        task_heartbeat,
        data_heartbeat,
        last_data_alert,
    )

    for task in list(tasks.values()):
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    tasks.clear()
    task_heartbeat.clear()
    data_heartbeat.clear()
    last_data_alert.clear()

    # Limpieza de caches de órdenes reales
    from core.orders import real_orders

    real_orders._CACHE_ORDENES = None
    real_orders._BUFFER_OPERACIONES.clear()
    real_orders._VENTAS_FALLIDAS.clear()
