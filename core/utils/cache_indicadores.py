from __future__ import annotations
from functools import wraps
from collections import OrderedDict
from typing import Any

_CACHE: OrderedDict[tuple, Any] = OrderedDict()
_MAX_SIZE = 512

def cached_indicator(func):
    """Decorador simple de cache para indicadores.

    Usa el timestamp final del DataFrame como parte de la clave.
    """
    @wraps(func)
    def wrapper(df, *args, **kwargs):
        ts = None
        if hasattr(df, "__len__") and len(df) > 0:
            if isinstance(df, dict) and "timestamp" in df:
                ts = df["timestamp"]
            elif "timestamp" in getattr(df, "columns", []):
                ts = df["timestamp"].iloc[-1]
            elif "close" in getattr(df, "columns", []):
                ts = hash(df["close"].values.tobytes())
            else:
                ts = len(df)
        key = (func.__name__, ts, args, tuple(sorted(kwargs.items())))
        if key in _CACHE:
            return _CACHE[key]
        resultado = func(df, *args, **kwargs)
        _CACHE[key] = resultado
        if len(_CACHE) > _MAX_SIZE:
            _CACHE.popitem(last=False)
        return resultado
    return wrapper