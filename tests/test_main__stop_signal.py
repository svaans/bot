import asyncio, importlib, sys, pytest

@pytest.mark.asyncio
async def test_main_recibe_parada(monkeypatch):
    monkeypatch.setenv("MODO_REAL", "0")
    sys.modules.pop("main", None)
    importlib.invalidate_caches()
    main = importlib.import_module("main")

    # Lanza main en paralelo y simula Ctrl+C enviando señal después de 0.3 s
    async def killer():
        await asyncio.sleep(0.3)
        # si tienes expuesto un helper en main, úsalo; si no, usa signal.raise_signal
        import signal, os
        os.kill(os.getpid(), signal.SIGINT)

    t = asyncio.create_task(main.main())
    await asyncio.wait_for(asyncio.gather(t, killer()), timeout=2.0)
