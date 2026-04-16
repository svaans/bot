"""Módulo solo para Vulture: símbolos “usados” vía imports (carga dinámica, etc.).

**No** es una whitelist de seguridad ni de red. Solo evita falsos positivos de
dead code.

Uso (desde la raíz del repo): ``python -m vulture`` — la configuración en
``pyproject.toml`` ya incluye ``.`` y este archivo. Equivalente manual::

    python -m vulture . vulture_whitelist.py

Importar este módulo en runtime de producción fuerza cargas innecesarias;
no lo referenciéis desde la aplicación.
"""

from __future__ import annotations

# -- Loaders dinámicos -----------------------------------------------------
# Estos módulos exponen atributos a través de ``__getattr__`` para reducir
# el tiempo de importación. Declararlos aquí evita falsos positivos.
from core import __getattr__ as core__getattr__  # noqa: F401
from core.orders import __getattr__ as orders__getattr__  # noqa: F401
from core.startup_manager import __getattr__ as startup__getattr__  # noqa: F401
from core.data import __getattr__ as data__getattr__  # noqa: F401
from indicators import __getattr__ as indicators__getattr__  # noqa: F401

# -- Dataclasses de persistencia -------------------------------------------
# Utilizadas desde C extensions o capas dinámicas, por lo que Vulture no
# detecta su uso directo.
from estado.evaluaciones_repo import EvaluacionRepositoryStats  # noqa: F401
from core.persistencia_tecnica import PersistenciaTecnica  # noqa: F401

__all__ = [
    "core__getattr__",
    "orders__getattr__",
    "startup__getattr__",
    "data__getattr__",
    "indicators__getattr__",
    "EvaluacionRepositoryStats",
    "PersistenciaTecnica",
]
