import asyncio
import types
import importlib
import pytest

@pytest.mark.asyncio
async def test_retries_and_notifier(monkeypatch, capsys):
    """
    La tarea del bot falla dos veces, a la tercera termina.
    Verificamos que main hace reintentos con backoff y finalmente finaliza sin errores.
    """
    attempts = {"n": 0}

    async def failing_task():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("fallo")
        # Termina rápido
        await asyncio.sleep(0)

    class FakeSM:
        async def run(self):
            # bot objeto simple, tarea como coroutine y config simulada
            return types.SimpleNamespace(notificador=None), failing_task(), types.SimpleNamespace(modo_real=False)

    # Acelerar sleeps sin recursión
    original_sleep = asyncio.sleep
    async def fast_sleep(s): 
        # ignora 's' y duerme 0
        await original_sleep(0)

    # Inyectar dependencias antes de importar main
    sysmods = importlib.import_module('sys').modules
    sysmods['core.startup_manager'] = types.SimpleNamespace(StartupManager=FakeSM)
    # Infra ligera
    class Exporter:
        def shutdown(self): pass
        def server_close(self): pass
    sysmods['core.metrics'] = types.SimpleNamespace(iniciar_exporter=lambda: Exporter())
    sysmods['core.supervisor'] = types.SimpleNamespace(start_supervision=lambda: None, stop_supervision=lambda: None)
    sysmods['core.hot_reload'] = types.SimpleNamespace(start_hot_reload=lambda *a, **k: object(), stop_hot_reload=lambda o: None)
    sysmods['core.notification_manager'] = types.SimpleNamespace(crear_notification_manager_desde_env=lambda: None)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    mod = importlib.reload(importlib.import_module('main'))
    await mod.main()
    out = capsys.readouterr().out

    # Deben aparecer al menos un reinicio y la salida ordenada
    assert "Reinicio del bot" in out
    assert "finalizado sin errores" in out

