"""Benchmark calculo de score técnico usando Rust"""
import timeit
import numpy as np
import pandas as pd
import core.scoring as sc


def crear_df(n=1000):
    rng = np.random.default_rng(0)
    base = rng.random(n) * 10 + 100
    highs = base + rng.random(n)
    lows = base - rng.random(n)
    return pd.DataFrame({"close": base, "high": highs, "low": lows})


def main():
    df = crear_df()
    args = {"rsi": 50.0, "momentum": 0.01, "slope": 0.001, "tendencia": "alcista", "symbol": "AAA"}
    sc.HAS_RUST = False
    t_py = timeit.timeit(lambda: sc.calcular_score_tecnico(df, **args), number=500)
    if sc._score_rust is None:
        print("Extensión Rust no disponible")
        return
    sc.HAS_RUST = True
    t_rust = timeit.timeit(lambda: sc.calcular_score_tecnico(df, **args), number=500)
    print(f"Python: {t_py:.4f}s")
    print(f"Rust:   {t_rust:.4f}s")


if __name__ == "__main__":
    main()