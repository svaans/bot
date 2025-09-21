from __future__ import annotations

from contextlib import suppress
from importlib import import_module

from .logger import configurar_logger
__all__ = ['configurar_logger']

with suppress(Exception):
    _utils = import_module('core.utils.utils')
    __all__.extend(name for name in dir(_utils) if not name.startswith('_'))
    globals().update({name: getattr(_utils, name) for name in __all__ if name != 'configurar_logger'})
