# tests/test_main_startup_contract.py
import types
import asyncio
import builtins
import importlib
import pytest

@pytest.mark.asyncio
async def test_startup_returns_coroutine_wrapped(monkeypatch, capsys):
    # fake StartupManager.run -> (bot, coro, config)
    class FakeSM:
        async def run(self):
            async def bot_loop(): await asyncio.sleep(0)
            return object(), bot_loop(), types.SimpleNamespace(modo_real=False)

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    mod = importlib.import_module('main')  # tu ruta real a main.py
    await mod.main()
    out = capsys.readouterr().out
    assert "Modo SIMULADO" in out

@pytest.mark.asyncio
async def test_startup_bad_tuple_size(monkeypatch, capsys):
    class FakeSM:
        async def run(self): return (object(),)  # tamaño 1

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out
    assert "no devolvió (bot, tarea_bot, config)" in out

@pytest.mark.asyncio
async def test_startup_task_type_error(monkeypatch, capsys):
    class FakeSM:
        async def run(self):
            return object(), 123, types.SimpleNamespace(modo_real=True)  # invalido

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out
    assert "tarea_bot no es Task ni coroutine" in out

@pytest.mark.asyncio
async def test_startup_clock_skew(monkeypatch, capsys):
    class FakeSM:
        async def run(self):
            raise RuntimeError("Desincronización de reloj")

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))
    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out
    assert "Desincronización de reloj detectada" in out
