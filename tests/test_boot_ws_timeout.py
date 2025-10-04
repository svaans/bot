import asyncio, importlib, sys, types, pytest

@pytest.mark.asyncio
async def test_ws_inactivo_lanza_runtimeerror(monkeypatch):
    # Forzamos modo real y timeout corto
    monkeypatch.setenv("MODO_REAL", "1")
    sys.modules.pop("core.startup_manager", None)
    sm_mod = importlib.import_module("core.startup_manager")

    # Trader con DataFeed que JAMÁS se activa
    dummy_feed = types.SimpleNamespace(activos=False)
    cfg = types.SimpleNamespace(ws_timeout=0.3)
    dummy_trader = types.SimpleNamespace(data_feed=dummy_feed, config=cfg, cliente=object())

    sm = sm_mod.StartupManager(trader=dummy_trader)
    with pytest.raises(RuntimeError):
        # Debe fallar por "WS no conectado" dentro del timeout
        await asyncio.wait_for(sm._wait_ws(cfg.ws_timeout), timeout=1.0)
