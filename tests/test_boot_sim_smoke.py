import asyncio, importlib, os, sys, pytest

@pytest.mark.asyncio
async def test_boot_sim_no_cuelga(monkeypatch):
    monkeypatch.setenv("MODO_REAL", "0")
    sys.modules.pop("main", None)
    importlib.invalidate_caches()
    main = importlib.import_module("main")
    await asyncio.wait_for(main.main(), timeout=3.0)
