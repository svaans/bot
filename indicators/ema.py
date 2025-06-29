"""EMA crossover utilities with optional Rust acceleration.

To build the Rust extension run:
```
maturin develop --release -m technical_indicators_rust/Cargo.toml
```
"""
import numpy as np
import pandas as pd

try:
    from numba import jit
except Exception:  # pragma: no cover - numba es opcional
    def jit(*args, **kwargs):  # type: ignore
        def wrapper(func):
            return func

        return wrapper
    
try:  # pragma: no cover - optional rust extension
    from technical_indicators_rust import ema as _ema_rust
    HAS_RUST = True
except Exception:  # pragma: no cover - missing rust module
    _ema_rust = None
    HAS_RUST = False


@jit(nopython=True)
def _ema_numba(valores: np.ndarray, periodo: int) -> np.ndarray:
    alpha = 2.0 / (periodo + 1.0)
    resultado = np.empty_like(valores)
    resultado[0] = valores[0]
    for i in range(1, valores.size):
        resultado[i] = alpha * valores[i] + (1 - alpha) * resultado[i - 1]
    return resultado

def calcular_cruce_ema(df: pd.DataFrame, rapida=12, lenta=26) -> bool:
    if len(df) < lenta + 2 or "close" not in df:
        return False
    
    close = df["close"].to_numpy(dtype=float)

    if HAS_RUST:
        fast_prev = _ema_rust(close[:-1], rapida)
        fast_curr = _ema_rust(close, rapida)
        slow_prev = _ema_rust(close[:-1], lenta)
        slow_curr = _ema_rust(close, lenta)
        cruce_anterior = fast_prev < slow_prev
        cruce_actual = fast_curr > slow_curr
        return cruce_anterior and cruce_actual

    if HAS_RUST:
        fast_prev = _ema_rust(close[:-1], rapida)
        fast_curr = _ema_rust(close, rapida)
        slow_prev = _ema_rust(close[:-1], lenta)
        slow_curr = _ema_rust(close, lenta)
        cruce_anterior = fast_prev < slow_prev
        cruce_actual = fast_curr > slow_curr
        return cruce_anterior and cruce_actual

    ema_fast = df["close"].ewm(span=rapida, adjust=False).mean()
    ema_slow = df["close"].ewm(span=lenta, adjust=False).mean()

    cruce_anterior = ema_fast.iloc[-2] < ema_slow.iloc[-2]
    cruce_actual = ema_fast.iloc[-1] > ema_slow.iloc[-1]

    return cruce_anterior and cruce_actual

def calcular_cruce_ema_fast(df: pd.DataFrame, rapida=12, lenta=26) -> bool:
    """Versión acelerada de :func:`calcular_cruce_ema` usando numba."""
    if len(df) < lenta + 2 or "close" not in df:
        return False

    close = df["close"].to_numpy(dtype=float)
    ema_fast = _ema_numba(close, rapida)
    ema_slow = _ema_numba(close, lenta)

    cruce_anterior = ema_fast[-2] < ema_slow[-2]
    cruce_actual = ema_fast[-1] > ema_slow[-1]

    return cruce_anterior and cruce_actual

