"""Benchmark del calculo TP/SL usando la extension en Rust"""
import timeit
import numpy as np
import pandas as pd
from core.adaptador_dinamico import (
    _calcular_tp_sl_adaptativos_py,
    calcular_tp_sl_adaptativos,
)


def crear_df(n=1000):
    data = {
        "high": np.random.random(n) * 100 + 100,
        "low": np.random.random(n) * 100,
        "close": np.random.random(n) * 100 + 50,
    }
    return pd.DataFrame(data)


def main():
    df = crear_df(2000)
    cfg = {"sl_ratio": 1.5, "tp_ratio": 2.5}
    t_py = timeit.timeit(lambda: _calcular_tp_sl_adaptativos_py("BTC/USDT", df, cfg), number=1000)
    t_fast = timeit.timeit(lambda: calcular_tp_sl_adaptativos("BTC/USDT", df, cfg), number=1000)
    print(f"Python puro: {t_py:.4f}s")
    print(f"Extension Rust: {t_fast:.4f}s")


if __name__ == "__main__":
    main()