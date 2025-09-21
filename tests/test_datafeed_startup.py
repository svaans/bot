# tests/test_datafeed_startup.py
import asyncio
import pytest
from core.data_feed import DataFeed

@pytest.mark.asyncio
async def test_datafeed_iniciar_sin_symbols_no_arranca(monkeypatch):
    """
    Iniciar el DataFeed sin símbolos ni handler debe registrar un warning y no lanzar excepciones.
    """
    df = DataFeed("1m")
    advertencias = []
    def fake_warning(msg, *args, **kwargs):
        advertencias.append(msg)
    # sustituimos el logger por una función que acumula mensajes
    from core.utils.logger import configurar_logger
    monkeypatch.setattr(df, "log", type("Fake", (), {"warning": fake_warning}))
    await df.iniciar()
    assert any("falta" in m for m in advertencias)

@pytest.mark.asyncio
async def test_datafeed_escuchar_arranca_consumers(monkeypatch):
    """
    Escuchar con símbolos y handler debe crear colas y tareas para cada símbolo.
    """
    df = DataFeed("1m")
    async def dummy_handler(candle):
        pass
    monkeypatch.setattr(df, "_stream_simple", lambda s: asyncio.sleep(0.01))
    await df.escuchar(["BTCUSDT", "ETHUSDT"], dummy_handler)
    # Hay dos colas y al menos dos tareas de consumidor
    assert set(df._queues.keys()) == {"BTCUSDT", "ETHUSDT"}
    assert all(not t.done() for t in df._consumer_tasks.values())
