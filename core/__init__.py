from importlib import import_module

from core.utils.asyncio_compat import ensure_taskgroup

ensure_taskgroup()

__all__ = ['data', 'risk', 'orders', 'strategies', 'utils', 'streams']


def __getattr__(name):
    if name in __all__:
        return import_module(f'.{name}', __name__)
    raise AttributeError(name)
