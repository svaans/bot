# tests/test_main_bootstrap_isolado.py
"""
Prueba aislada del bootstrap de main.py sin depender de implementaciones reales.
- Instala stubs SOLO durante este archivo y los limpia al finalizar.
- Verifica que main.main() arranca y termina con una tarea corta.
- Verifica que run_with_optional_aiomonitor() hace fallback si no hay aiomonitor.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
import types
from contextlib import contextmanager

import pytest


# ---------- Utilidades de aislamiento de módulos ----------

@contextmanager
def patch_sys_module(name: str, module: types.ModuleType):
    """Inserta temporalmente un módulo en sys.modules y lo restaura al salir."""
    prev = sys.modules.get(name, None)
    sys.modules[name] = module
    try:
        yield
    finally:
        if prev is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = prev


@pytest.fixture(autouse=True)
def _isolate_and_stub_modules(monkeypatch):
    """
    Instala stubs mínimos para dependencias llamadas desde main.py.
    Se limpian al final para no afectar a otros tests.
    """
    # ---- core.hot_reload ----
    core_hot_reload = types.ModuleType("core.hot_reload")
    def _stub_start_hot_reload(path=None, modules=None):
        class _Obs: pass
        return _Obs()
    def _stub_stop_hot_reload(observer):
        return None
    core_hot_reload.start_hot_reload = _stub_start_hot_reload
    core_hot_reload.stop_hot_reload = _stub_stop_hot_reload

    # ---- core.supervisor ----
    core_supervisor = types.ModuleType("core.supervisor")
    async def _stub_start_supervision():
        return None
    async def _stub_stop_supervision():
        return None
    core_supervisor.start_supervision = _stub_start_supervision
    core_supervisor.stop_supervision = _stub_stop_supervision

    # ---- core.metrics ----
    core_metrics = types.ModuleType("core.metrics")
    class _ExporterServer:
        def shutdown(self): pass
        def server_close(self): pass
    def _stub_iniciar_exporter():
        return _ExporterServer()
    core_metrics.iniciar_exporter = _stub_iniciar_exporter

    # ---- core.notification_manager ----
    core_notifier = types.ModuleType("core.notification_manager")
    def _stub_crear_notification_manager_desde_env():
        class _N:
            def enviar(self, texto, nivel="INFO"):
                # No hace I/O real; devuelve tupla para trazabilidad si se necesitara.
                return (texto, nivel)
        return _N()
    core_notifier.crear_notification_manager_desde_env = _stub_crear_notification_manager_desde_env

    # ---- core.startup_manager ----
    core_startup = types.ModuleType("core.startup_manager")
    class _Cfg:
        modo_real = False
    class _Bot:
        def solicitar_parada(self): pass
        async def cerrar(self): await asyncio.sleep(0)
    class StartupManager:
        def __init__(self, *a, **k): pass
        async def run(self):
            async def tarea():
                # tarea principal que termina sola rápidamente
                await asyncio.sleep(0.05)
            return _Bot(), asyncio.create_task(tarea()), _Cfg()
    core_startup.StartupManager = StartupManager

    # Instala todos los stubs con limpieza garantizada
    with patch_sys_module("core.hot_reload", core_hot_reload), \
         patch_sys_module("core.supervisor", core_supervisor), \
         patch_sys_module("core.metrics", core_metrics), \
         patch_sys_module("core.notification_manager", core_notifier), \
         patch_sys_module("core.startup_manager", core_startup):
        # Invalidar cachés de import para que main.py coja estos stubs
        importlib.invalidate_caches()
        yield
    # Al salir, los patch_sys_module restauran sys.modules → no contamina


# ---------- Tests ----------

@pytest.mark.asyncio
async def test_main_arranca_y_termina_ok(monkeypatch):
    """
    Verifica que main.main() arranca con stubs y finaliza sin bloquear.
    Se impone un timeout estricto por seguridad.
    """
    # Forzar que si main lee variables de entorno, no habilite modos especiales
    monkeypatch.delenv("AIOMONITOR", raising=False)

    # (Re)importar main con los stubs presentes
    sys.modules.pop("main", None)
    importlib.invalidate_caches()
    main = importlib.import_module("main")

    # Ejecutar main.main() con timeout para evitar bloqueos inesperados
    await asyncio.wait_for(main.main(), timeout=1.0)


def test_runner_aiomonitor_fallback_sin_aiomonitor(monkeypatch):
    """
    Con AIOMONITOR=1 y forzando ImportError de 'aiomonitor',
    run_with_optional_aiomonitor() debe usar asyncio.run() (fallback) sin importar el paquete real.
    """
    import importlib

    monkeypatch.setenv("AIOMONITOR", "1")

    # Forzar ImportError para 'aiomonitor' incluso si está instalado
    _orig_import_module = importlib.import_module
    def _fake_import_module(name, package=None):
        if name == "aiomonitor":
            raise ImportError("forced by test")
        return _orig_import_module(name, package)
    monkeypatch.setattr(importlib, "import_module", _fake_import_module, raising=True)

    # Reimportar main limpio
    sys.modules.pop("main", None)
    importlib.invalidate_caches()
    main = importlib.import_module("main")

    ran = {"ok": False}
    async def _coro(): ran["ok"] = True

    main.run_with_optional_aiomonitor(_coro())
    assert ran["ok"] is True


