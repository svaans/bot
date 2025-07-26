import importlib
import pkgutil
import indicators

__all__ = []

for _, name, _ in pkgutil.iter_modules(indicators.__path__):
    module = importlib.import_module(f'indicators.{name}')
    globals()[name] = module
    __all__.append(name)