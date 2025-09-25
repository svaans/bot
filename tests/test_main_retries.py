# tests/test_main_retries.py
import asyncio, types, importlib, pytest

@pytest.mark.asyncio
async def test_retries_and_notifier(monkeypatch, capsys):
    attempts = {"n": 0}
    async def failing_task():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("fallo")
        await asyncio.sleep(0)

    class FakeSM:
        async def run(self):
            return types.SimpleNamespace(notificador=None), failing_task(), types.SimpleNamespace(modo_real=False)

    # acelerar sleeps
    async def fast_sleep(s): await asyncio.sleep(0)

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out
    assert "Reinicio del bot" in out
    assert "finalizado sin errores" in out
