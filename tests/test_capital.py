import asyncio
from types import SimpleNamespace

import pytest

from core.capital_manager import CapitalManager


def _crear_manager() -> CapitalManager:
    config = SimpleNamespace(symbols=['BTC/USDT'], modo_capital_bajo=False, capital_currency='USDT')
    risk = SimpleNamespace(umbral=1.0)
    return CapitalManager(config, cliente=None, risk=risk, fraccion_kelly=0.1)


def test_validar_minimos():
    cm = _crear_manager()
    ok, msg = cm._validar_minimos(5, 10, None)
    assert not ok and 'Orden mínima' in msg
    ok, msg = cm._validar_minimos(15, 10, 20)
    assert not ok and 'mínimo Binance' in msg
    ok, msg = cm._validar_minimos(25, 10, 20)
    assert ok and msg is None


@pytest.mark.asyncio
async def test_actualizar_capital_async():
    cm = _crear_manager()
    fut = asyncio.get_event_loop().create_future()
    await cm._on_actualizar_capital({'symbol': 'BTC/USDT', 'retorno_total': 0.1, 'future': fut})
    assert fut.result() == pytest.approx(1100.0)
    assert cm.capital_por_simbolo['BTC/USDT'] == pytest.approx(1100.0)