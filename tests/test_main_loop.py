import builtins
import asyncio
import importlib
import types
import pytest

@pytest.mark.asyncio
async def test_main_reintenta_si_startup_falla(monkeypatch):
    """
    Si StartupManager.run() lanza una excepción, main debe imprimir un mensaje y no quedarse pillado.
    Este test inyecta el stub ANTES de importar/reload main para garantizar que se usa.
    """
    # Inyectar stub de StartupManager antes de importar main
    class StubStartup:
        async def run(self):
            raise RuntimeError("Mock error")

    import sys as _sys
    _sys.modules['core.startup_manager'] = types.SimpleNamespace(StartupManager=StubStartup)

    # Evitar cargas reales de infraestructura
    class Exporter:
        def shutdown(self): pass
        def server_close(self): pass
    _sys.modules['core.metrics'] = types.SimpleNamespace(iniciar_exporter=lambda: Exporter())
    _sys.modules['core.supervisor'] = types.SimpleNamespace(start_supervision=lambda: None, stop_supervision=lambda: None)
    _sys.modules['core.hot_reload'] = types.SimpleNamespace(start_hot_reload=lambda *a, **k: object(), stop_hot_reload=lambda o: None)
    _sys.modules['core.notification_manager'] = types.SimpleNamespace(crear_notification_manager_desde_env=lambda: None)

    # Importar/reload main tras inyectar stubs
    main_mod = importlib.reload(importlib.import_module('main'))

    # Capturar prints
    mensajes = []
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: mensajes.append(" ".join(str(a) for a in args)))

    # Ejecutar con timeout para evitar cuelgues
    try:
        await asyncio.wait_for(main_mod.main(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("main() se quedó colgado ante fallo de startup")

    assert any("Mock error" in m for m in mensajes), "main() debería reportar el fallo del StartupManager"
