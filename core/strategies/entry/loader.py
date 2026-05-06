"""Cargador de estrategias de entrada.

Descubre automáticamente los módulos ``estrategia_*.py`` dentro del paquete
``core.strategies.entry`` e importa la función homónima de cada uno.

Cambio respecto a la implementación anterior:
- Sustituye ``importlib.util.spec_from_file_location`` + ``exec_module`` por
  ``importlib.import_module``, que registra correctamente el módulo en
  ``sys.modules`` y garantiza que los imports relativos dentro de cada
  estrategia (p.ej. ``from .validaciones_comunes import …``) resuelvan
  siempre, independientemente del orden de importación.
"""
from __future__ import annotations

import importlib
import os

from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger

log = configurar_logger("loader_entradas")

_PACKAGE = "core.strategies.entry"

EXCLUIR_PREFIX = (
    "__", "gestor", "loader", "analisis", "validadores",
    "validaciones", "validador", "verificar"
)


def _es_archivo_estrategia(nombre: str) -> bool:
    return nombre.endswith(".py") and not nombre.startswith(EXCLUIR_PREFIX)


def cargar_estrategias() -> dict:
    """Carga todas las estrategias de entrada del paquete ``core.strategies.entry``.

    Itera los archivos ``.py`` del directorio, importa cada módulo con
    ``importlib.import_module`` y extrae la función cuyo nombre coincide con el
    del módulo.

    Returns
    -------
    dict
        Mapa ``{nombre_estrategia: funcion}``.
    """
    estrategias: dict = {}
    ruta_base = os.path.dirname(__file__)

    for archivo in sorted(os.listdir(ruta_base)):
        if not _es_archivo_estrategia(archivo):
            continue

        nombre_modulo = archivo[:-3]
        full_name = f"{_PACKAGE}.{nombre_modulo}"

        try:
            module_obj = importlib.import_module(full_name)
        except Exception as exc:
            log.error(
                "❌ Error importando %s: %s",
                nombre_modulo,
                format_exception_for_log(exc),
            )
            continue

        func = getattr(module_obj, nombre_modulo, None)
        if callable(func):
            estrategias[nombre_modulo] = func
        else:
            log.warning("⚠️ %s no expone función `%s()`", nombre_modulo, nombre_modulo)

    log.info("🧩 Estrategias cargadas: %d", len(estrategias))
    if not estrategias:
        log.warning("⚠️ No se cargó ninguna estrategia. Revisa exclusiones y errores arriba.")

    return estrategias
