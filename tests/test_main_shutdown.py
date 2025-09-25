# tests/test_main_shutdown.py
import asyncio, types, importlib, pytest

@pytest.mark.asyncio
async def test_graceful_shutdown(monkeypatch, capsys):
    calls = {"sol_parada":0, "cerrar":0}
    class Bot:
        def solicitar_parada(self): calls["sol_parada"] += 1
        async def cerrar(self): 
            calls["cerrar"] += 1
            await asyncio.sleep(0)

    async def bot_loop():
        # espera hasta que el test cancele o se pida parada
        await asyncio.sleep(0.01)

    class FakeSM:
        async def run(self):
            return Bot(), bot_loop(), types.SimpleNamespace(modo_real=False)

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    mod = importlib.reload(importlib.import_module('main'))
    # ejecutamos main pero cancelamos tras un breve tiempo para forzar la rama de finally
    task = asyncio.create_task(mod.main())
    await asyncio.sleep(0.005)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls["sol_parada"] >= 1
    assert calls["cerrar"] >= 1
