"""Benchmark de ATR: C++ vs Rust"""
import timeit
import numpy as np
import pandas as pd
from indicators.atr import calcular_atr_fast
from fast_indicators_rust import atr as atr_rust


def crear_df(n=2000):
    data = {
        "high": np.random.random(n) * 100 + 100,
        "low": np.random.random(n) * 100,
        "close": np.random.random(n) * 100 + 50,
    }
    return pd.DataFrame(data)


def main():
    df = crear_df()
    t_cpp = timeit.timeit(lambda: calcular_atr_fast(df, 14), number=1000)
    t_rust = timeit.timeit(lambda: atr_rust(df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy(), 14), number=1000)
    print(f"C++ extension: {t_cpp:.4f}s")
    print(f"Rust extension: {t_rust:.4f}s")


if __name__ == "__main__":
    main()