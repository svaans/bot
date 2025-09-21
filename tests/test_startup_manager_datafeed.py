# tests/test_startup_manager_datafeed.py
import asyncio
import pytest
from core.startup_manager import StartupManager

class DummyTrader:
    async def run(self): await asyncio.sleep(0)
    def _procesar_vela(self, c): pass

class DummyFeed:
    def __init__(self): self.called = []
    async def iniciar(self): self.called.append("iniciar")
    async def escuchar(self, symbols, handler, cliente=None): self.called.append("escuchar")
    def verificar_continuidad(self): return True

@pytest.mark.asyncio
async def test_startup_manager_arranca_datafeed(monkeypatch):
    feed = DummyFeed()
    trader = DummyTrader()
    # Sustituye StartupManager para que use nuestro feed y trader
    sm = StartupManager(data_feed=feed, trader=trader)
    # Config mínima
    sm.config = type("Cfg", (), {"symbols":["BTCUSDT"], "intervalo_velas":"1m"})
    await sm._bootstrap()
    await sm._open_streams()
    # Debe haber llamado a iniciar() o escuchar()
    assert feed.called, "DataFeed no fue arrancado por _open_streams"
