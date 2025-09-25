# tests/test_main_infra_cleanup.py
import types
import importlib
import pytest

@pytest.mark.asyncio
async def test_hot_reload_fails_triggers_cleanup(monkeypatch, capsys):
    # fakes
    class Exporter:
        def __init__(self): self.closed = False
        def shutdown(self): self.closed = True
        def server_close(self): self.closed = True

    async def start_supervision(): return "ok"
    async def stop_supervision(): print("supervision stopped")

    def start_hot_reload(*a, **k): raise RuntimeError("boom")

    # patch infra
    core_metrics = types.SimpleNamespace(iniciar_exporter=lambda: Exporter())
    core_supervisor = types.SimpleNamespace(start_supervision=start_supervision, stop_supervision=stop_supervision)
    core_hot = types.SimpleNamespace(start_hot_reload=start_hot_reload, stop_hot_reload=lambda o: None)

    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.metrics', core_metrics)
    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.supervisor', core_supervisor)
    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.hot_reload', core_hot)

    # StartupManager feliz para llegar a infra
    class FakeSM:
        async def run(self): 
            async def task(): await asyncio.sleep(0.001)
            return object(), task(), types.SimpleNamespace(modo_real=False)
    monkeypatch.setitem(importlib.import_module('sys').modules, 'core.startup_manager', types.SimpleNamespace(StartupManager=FakeSM))

    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out
    assert "Fallo durante la inicialización de infraestructura" in out
