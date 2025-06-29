import asyncio
from datetime import datetime, timezone

import pytest

from core.procesar_vela import procesar_vela


class DummyOrders:
    def obtener(self, symbol):
        return False


class DummyWatchdog:
    def ping(self, name):
        pass


class DummyTrader:
    def __init__(self):
        estado = type(
            "E", (), {"buffer": [], "ultimo_timestamp": None, "tendencia_detectada": None}
        )
        self.estado = {"AAA": estado()}
        self.watchdog = DummyWatchdog()
        self.fecha_actual = datetime.now(timezone.utc).date()
        self.ajustar_capital_diario = lambda: None
        self.orders = DummyOrders()
        self.estado_tendencia = {}
        self.notificador = None
        self.actualizar_fraccion_kelly = lambda: None
        self.config = type("C", (), {"symbols": ["AAA"]})()

        async def _verificar_salidas(symbol, df):
            await asyncio.sleep(0)

        async def evaluar_condiciones_entrada(symbol, df):
            await asyncio.sleep(0)

        self._verificar_salidas = _verificar_salidas
        self.evaluar_condiciones_entrada = evaluar_condiciones_entrada


@pytest.mark.asyncio
async def test_procesar_vela_concurrente():
    trader = DummyTrader()
    tareas = []
    for i in range(50):
        vela = {"symbol": "AAA", "timestamp": i, "close": 1}
        tareas.append(asyncio.create_task(procesar_vela(trader, vela)))
    await asyncio.wait_for(asyncio.gather(*tareas), timeout=30)
    assert all(t.done() for t in tareas)