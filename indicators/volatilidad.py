"""Volatility related indicators with optional Rust acceleration.

Build the Rust extension via:
```
maturin develop --release -m technical_indicators_rust/Cargo.toml
```
"""
import pandas as pd
import numpy as np

try:  # pragma: no cover - optional rust extension
    from technical_indicators_rust import volatility as _volatility_rust
    HAS_RUST = True
except Exception:  # pragma: no cover - missing rust module
    _volatility_rust = None
    HAS_RUST = False

def calcular_volatility_breakout(df: pd.DataFrame, ventana: int = 14, factor: float = 1.5) -> bool:
    if "high" not in df or "low" not in df or "close" not in df or len(df) < ventana + 1:
        return False

    df = df.copy()
    rango = df["high"] - df["low"]
    rango_medio = rango.rolling(window=ventana).mean()
    breakout = rango.iloc[-1] > rango_medio.iloc[-2] * factor

    return breakout

def calcular_volatilidad(df: pd.DataFrame, periodo: int = 14) -> float:
    """Devuelve la volatilidad (desviación estándar) del cierre."""
    if "close" not in df or len(df) < periodo:
        return None

    close = df["close"].to_numpy(dtype=float)

    if HAS_RUST:
        valor = _volatility_rust(close, periodo)
        return float(valor) if valor == valor else None

    return float(np.std(close[-periodo:], ddof=0))