# tests/test_main_startup_contract.py
import types
import asyncio
import importlib
import os
import pytest

def _install_infra_fakes(monkeypatch):
    """Inserta módulos fake en sys.modules ANTES de importar main."""
    # Exporter fake
    class Exporter:
        def __init__(self): self.closed = False
        def shutdown(self): self.closed = True
        def server_close(self): self.closed = True

    core_metrics = types.SimpleNamespace(
        iniciar_exporter=lambda: Exporter()
    )
    core_supervisor = types.SimpleNamespace(
        start_supervision=lambda: None,
        stop_supervision=lambda: None,
    )
    core_hot_reload = types.SimpleNamespace(
        start_hot_reload=lambda *a, **k: object(),
        stop_hot_reload=lambda o: None,
    )
    core_notification_manager = types.SimpleNamespace(
        crear_notification_manager_desde_env=lambda: None
    )

    sysmods = importlib.import_module('sys').modules
    sysmods['core.metrics'] = core_metrics
    sysmods['core.supervisor'] = core_supervisor
    sysmods['core.hot_reload'] = core_hot_reload
    sysmods['core.notification_manager'] = core_notification_manager

    # Evitar caminos de depuración que no están instalados en CI
    monkeypatch.delenv("AIOMONITOR", raising=False)
    monkeypatch.delenv("DEBUGPY", raising=False)

@pytest.mark.asyncio
async def test_startup_returns_coroutine_wrapped(monkeypatch, capsys):
    # Fake de StartupManager.run -> (bot, coro que termina, config)
    class FakeSM:
        async def run(self):
            async def bot_loop():
                await asyncio.sleep(0)  # termina rápido
            return object(), bot_loop(), types.SimpleNamespace(modo_real=False)

    _install_infra_fakes(monkeypatch)
    import sys as _sys
    _sys.modules['core.startup_manager'] = types.SimpleNamespace(StartupManager=FakeSM)

    mod = importlib.import_module('main')  # importa después de inyectar fakes
    await asyncio.wait_for(mod.main(), timeout=1.0)
    out = capsys.readouterr().out
    assert "Modo SIMULADO" in out

@pytest.mark.asyncio
async def test_startup_bad_tuple_size(monkeypatch, capsys):
    class FakeSM:
        async def run(self):
            return (object(),)  # tamaño 1 -> debe fallar

    _install_infra_fakes(monkeypatch)
    import sys as _sys
    _sys.modules['core.startup_manager'] = types.SimpleNamespace(StartupManager=FakeSM)

    # reload para coger el módulo con los fakes
    mod = importlib.reload(importlib.import_module('main'))
    await asyncio.wait_for(mod.main(), timeout=1.0)
    out = capsys.readouterr().out
    assert "no devolvió (bot, tarea_bot, config)" in out

@pytest.mark.asyncio
async def test_startup_task_type_error(monkeypatch, capsys):
    class FakeSM:
        async def run(self):
            # tarea_bot inválida -> debe quejarse
            return object(), 123, types.SimpleNamespace(modo_real=True)

    _install_infra_fakes(monkeypatch)
    import sys as _sys
    _sys.modules['core.startup_manager'] = types.SimpleNamespace(StartupManager=FakeSM)

    mod = importlib.reload(importlib.import_module('main'))
    await asyncio.wait_for(mod.main(), timeout=1.0)
    out = capsys.readouterr().out
    assert "tarea_bot no es Task ni coroutine" in out

@pytest.mark.asyncio
async def test_startup_clock_skew(monkeypatch, capsys):
    class FakeSM:
        async def run(self):
            raise RuntimeError("Desincronización de reloj")

    _install_infra_fakes(monkeypatch)
    import sys as _sys
    _sys.modules['core.startup_manager'] = types.SimpleNamespace(StartupManager=FakeSM)

    mod = importlib.reload(importlib.import_module('main'))
    await asyncio.wait_for(mod.main(), timeout=1.0)
    out = capsys.readouterr().out
    assert "Desincronización de reloj detectada" in out

