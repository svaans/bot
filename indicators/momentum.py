"""Momentum indicator with optional Rust implementation.

Compile the Rust module using:
```
maturin develop --release -m technical_indicators_rust/Cargo.toml
```
"""
import pandas as pd
import numpy as np
from core.utils.cache_indicadores import cached_indicator

try:  # pragma: no cover - optional rust extension
    from technical_indicators_rust import momentum as _momentum_rust
    HAS_RUST = True
except Exception:  # pragma: no cover - missing rust module
    _momentum_rust = None
    HAS_RUST = False

@cached_indicator
def calcular_momentum(df: pd.DataFrame, periodo: int = 10) -> float:
    if "close" not in df or len(df) < periodo + 1:
        return None

    close = df["close"].to_numpy(dtype=float)

    if HAS_RUST:
        valor = _momentum_rust(close, periodo)
        return float(valor) if valor == valor else None
    
    return float(close[-1] - close[-periodo])

