import asyncio, importlib, sys, types, pytest

@pytest.mark.asyncio
async def test_bootstrap_backfill_timeout(monkeypatch):
    # Stub warmup_inicial muy lento
    modname = "core.data.bootstrap"
    sys.modules.pop(modname, None)
    fake = types.ModuleType(modname)
    async def warmup_inicial(*a, **k): await asyncio.sleep(5)  # simula bloqueo
    fake.warmup_inicial = warmup_inicial
    sys.modules[modname] = fake

    # Importa StartupManager y llama a _bootstrap con timeout
    sm_mod = importlib.import_module("core.startup_manager")
    sm = sm_mod.StartupManager(trader=types.SimpleNamespace(config=types.SimpleNamespace()))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sm._bootstrap(), timeout=0.5)
