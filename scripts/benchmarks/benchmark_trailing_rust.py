"""Benchmark de verificación de trailing stop con y sin Rust"""
import timeit
import numpy as np
import pandas as pd
from core.strategies.exit import salida_trailing_stop as st


def crear_df(n=1000):
    base = np.linspace(100, 101, n)
    highs = base + 1
    lows = base - 1
    return pd.DataFrame({"close": base, "high": highs, "low": lows})


def main():
    df = crear_df()
    info = {"precio_entrada": 100, "max_price": 100, "direccion": "long"}
    config = {"trailing_start_ratio": 1.01, "trailing_distance_ratio": 0.02}
    st.HAS_RUST = False
    t_py = timeit.timeit(lambda: st.verificar_trailing_stop(info.copy(), df["close"].iloc[-1], df, config), number=500)
    if not hasattr(st, "_verificar_trailing_stop_rust"):
        print("Extensión Rust no disponible")
        return
    st.HAS_RUST = True
    t_rust = timeit.timeit(lambda: st.verificar_trailing_stop(info.copy(), df["close"].iloc[-1], df, config), number=500)
    print(f"Python: {t_py:.4f}s")
    print(f"Rust:   {t_rust:.4f}s")


if __name__ == "__main__":
    main()