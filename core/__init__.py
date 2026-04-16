"""Paquete principal del bot.

Al importarse :mod:`core` se aplica :func:`core.utils.asyncio_compat.ensure_taskgroup`
para que ``asyncio.TaskGroup`` exista en Python < 3.11 (parche idempotente).

Si ejecutáis código que use ``TaskGroup`` sin haber importado antes ``core``,
haced ``import core`` o ``import core.utils.asyncio_compat`` al inicio. Con
``python -I`` el arranque de ``site`` no carga hooks de usuario; este proyecto
no usa :file:`sitecustomize.py` por ese motivo.
"""

from importlib import import_module

from core.utils.asyncio_compat import ensure_taskgroup

ensure_taskgroup()

__all__ = ["data", "risk", "orders", "strategies", "utils", "streams"]


def __getattr__(name):
    if name in __all__:
        return import_module(f".{name}", __name__)
    raise AttributeError(name)
