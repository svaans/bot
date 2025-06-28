import pandas as pd
import timeit
from pathlib import Path

from indicators.atr import calcular_atr
from fast_indicators import atr as atr_cpp
try:
    from fast_indicators_rust import atr as atr_rust
    HAS_RUST = True
except Exception:
    atr_rust = None
    HAS_RUST = False


def load_large_dataset() -> pd.DataFrame:
    """Load the largest available parquet dataset under ``datos/``."""
    base_dir = Path(__file__).resolve().parents[2] / "datos"
    if not base_dir.exists():
        raise FileNotFoundError(f"Directorio de datos no encontrado: {base_dir}")
    parquet_files = list(base_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {base_dir}")
    file_path = max(parquet_files, key=lambda p: p.stat().st_size)
    return pd.read_parquet(file_path)


def benchmark(df: pd.DataFrame, numero: int = 200) -> None:
    """Run benchmarks on the provided dataframe and print a simple table."""
    setup = "from __main__ import df, calcular_atr, atr_cpp, atr_rust, HAS_RUST"
    tiempo_pandas = timeit.timeit("calcular_atr(df)", setup=setup, number=numero)
    tiempo_cpp = timeit.timeit(
        "atr_cpp(df['high'].to_numpy(), df['low'].to_numpy(), df['close'].to_numpy(), 14)",
        setup=setup,
        number=numero,
    )
    tiempo_rust = None
    if HAS_RUST:
        tiempo_rust = timeit.timeit(
            "atr_rust(df['high'].to_numpy(), df['low'].to_numpy(), df['close'].to_numpy(), 14)",
            setup=setup,
            number=numero,
        )

    print("\nResultados (\u03BCs por iteraci\u00f3n):")
    header = ["Implementaci\u00f3n", "Tiempo (s)"]
    rows = [
        ["Pandas", f"{tiempo_pandas:.6f}"],
        ["C++", f"{tiempo_cpp:.6f}"],
    ]
    if tiempo_rust is not None:
        rows.append(["Rust", f"{tiempo_rust:.6f}"])
    width_impl = max(len(r[0]) for r in rows + [header])
    width_time = max(len(r[1]) for r in rows + [header])
    sep = f"| {'-' * width_impl} | {'-' * width_time} |"
    print(f"| {header[0].ljust(width_impl)} | {header[1].rjust(width_time)} |")
    print(sep)
    for r in rows:
        print(f"| {r[0].ljust(width_impl)} | {r[1].rjust(width_time)} |")


def main() -> None:
    df = load_large_dataset()
    benchmark(df)


if __name__ == "__main__":
    main()