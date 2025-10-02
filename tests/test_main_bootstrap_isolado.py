import asyncio
import sys
import types
import os
import builtins
import pytest

# -------------------------
# 1) Stubs para core.*
# -------------------------
# Hot reload
core_hot_reload = types.ModuleType("core.hot_reload")
def _stub_start_hot_reload(path=None, modules=None):
    class _Obs: pass
    return _Obs()
def _stub_stop_hot_reload(observer):
    return None
core_hot_reload.start_hot_reload = _stub_start_hot_reload
core_hot_reload.stop_hot_reload = _stub_stop_hot_reload

# Supervisor
core_supervisor = types.ModuleType("core.supervisor")
async def _stub_start_supervision():
    return None
async def _stub_stop_supervision():
    return None
core_supervisor.start_supervision = _stub_start_supervision
core_supervisor.stop_supervision = _stub_stop_supervision

# Notification manager
core_notifier = types.ModuleType("core.notification_manager")
def _stub_crear_notification_manager_desde_env():
    class _N:
        def enviar(self, texto, nivel="INFO"):
            return (texto, nivel)
    return _N()
core_notifier.crear_notification_manager_desde_env = _stub_crear_notification_manager_desde_env

# Exporter/métricas
core_metrics = types.ModuleType("core.metrics")
class _ExporterServer:
    def shutdown(self): pass
    def server_close(self): pass
def _stub_iniciar_exporter():
    return _ExporterServer()
core_metrics.iniciar_exporter = _stub_iniciar_exporter

# StartupManager que devuelve (bot, tarea_bot, config) como exige main.py
core_startup = types.ModuleType("core.startup_manager")
class _Cfg:
    modo_real = False
class _Bot:
    def solicitar_parada(self): pass
    async def cerrar(self): await asyncio.sleep(0)
class StartupManager:
    async def run(self):
        async def tarea():
            # tarea principal del bot que termina sola rápidamente
            await asyncio.sleep(0.05)
        return _Bot(), asyncio.create_task(tarea()), _Cfg()
core_startup.StartupManager = StartupManager

# Inyectar módulos stub en sys.modules ANTES de importar main
sys.modules["core.hot_reload"] = core_hot_reload
sys.modules["core.supervisor"] = core_supervisor
sys.modules["core.notification_manager"] = core_notifier
sys.modules["core.metrics"] = core_metrics
sys.modules["core.startup_manager"] = core_startup

# -------------------------
# 2) Importar main.py
# -------------------------
import importlib
main = importlib.import_module("main")

@pytest.mark.asyncio
async def test_main_arranca_y_termina_ok():
    """
    Verifica que main.main():
      - Arranca con stubs.
      - Espera a que la tarea principal finalice.
      - Ejecuta la secuencia de apagado sin excepciones.
    """
    await main.main()  # si algo falla, pytest capturará la excepción

def test_runner_aiomonitor_fallback_sin_aiomonitor(monkeypatch):
    """
    Con AIOMONITOR=1 y sin el paquete instalado, el runner debe hacer fallback a asyncio.run(),
    tal y como contempla run_with_optional_aiomonitor().
    """
    monkeypatch.setenv("AIOMONITOR", "1")

    # Asegurar que 'aiomonitor' NO está importable
    if "aiomonitor" in sys.modules:
        del sys.modules["aiomonitor"]

    ran = {"ok": False}
    async def _coro():
        ran["ok"] = True

    main.run_with_optional_aiomonitor(_coro())
    assert ran["ok"] is True
