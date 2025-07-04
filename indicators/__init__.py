from importlib import import_module
import sys
for mod in ('ema', 'rsi'):
    module = import_module(f'indicators.{mod}')
    sys.modules[f'indicators.{mod}'] = module
    globals()[mod] = module
__all__ = ['ema', 'rsi']
