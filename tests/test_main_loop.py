# tests/test_main_loop.py
import asyncio
import builtins
import types
import pytest

@pytest.mark.asyncio
async def test_main_reintenta_si_startup_falla(monkeypatch):
    """
    Si StartupManager.run() lanza una excepción, main debe imprimir un mensaje y no quedarse pillado.
    """
    from main import main as main_coroutine
    # Stub de StartupManager que siempre falla
    class StubStartup:
        def __init__(self, *a, **k): pass
        async def run(self):
            raise RuntimeError("Mock error")
    # Parchamos la creación de StartupManager
    monkeypatch.setattr("core.startup_manager.StartupManager", StubStartup)
    # Capturamos prints
    mensajes = []
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: mensajes.append(" ".join(str(a) for a in args)))
    # Ejecutamos main; debe terminar rápido y no entrar en bucle infinito
    try:
        await asyncio.wait_for(main_coroutine(), timeout=1)
    except asyncio.TimeoutError:
        pytest.fail("main() se quedó colgado ante fallo de startup")
    assert any("Mock error" in m for m in mensajes), "main() debería reportar el fallo del StartupManager"
