"""Informe out-of-sample y walk-forward sobre historial de órdenes (Fase 3).

Usa la misma descomposición por estrategia que el entrenador
(:func:`learning.entrenador_estrategias.metricas_por_estrategia_desde_ordenes`).

- **Split único:** igual que :func:`learning.entrenador_estrategias.dividir_train_test`
  (último ``test_ratio`` del tiempo = validación).
- **Walk-forward:** el histórico se parte en ``n_folds`` bloques cronológicos; en cada paso
  ``k>=1`` el train son los bloques ``0..k-1`` y el test es el bloque ``k`` (sin mirar el futuro).

Ejemplos::

    python -m core.diag.informe_oos --symbol BTC/USDT
    python -m core.diag.informe_oos --walk-forward --n-folds 4
    python -m core.diag.informe_oos --out logs/oos
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

from learning.entrenador_estrategias import (
    dividir_train_test,
    metricas_por_estrategia_desde_ordenes,
)
from learning.historial_operaciones import (
    CARPETA_ORDENES,
    cargar_historial_operaciones,
    symbol_desde_parquet_stem,
)


def _ordenar_cronologico(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("timestamp", "fecha_cierre"):
        if col in df.columns:
            return df.sort_values(col).reset_index(drop=True)
    return df.reset_index(drop=True)


def walk_forward_splits(
    df: pd.DataFrame,
    n_folds: int = 4,
    *,
    min_train_rows: int = 5,
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """Pares (train, test) cronológicos expandiendo el train y avanzando el test."""

    n = len(df)
    if n_folds < 2 or n < n_folds + min_train_rows:
        return []

    chunk = n // n_folds
    chunks: list[list[int]] = []
    start = 0
    for i in range(n_folds):
        end = start + chunk if i < n_folds - 1 else n
        chunks.append(list(range(start, end)))
        start = end

    pairs: list[tuple[pd.DataFrame, pd.DataFrame]] = []
    for k in range(1, n_folds):
        train_ix = [i for j in range(k) for i in chunks[j]]
        test_ix = chunks[k]
        if len(train_ix) < min_train_rows or len(test_ix) < 1:
            continue
        pairs.append(
            (
                df.iloc[train_ix].reset_index(drop=True),
                df.iloc[test_ix].reset_index(drop=True),
            )
        )
    return pairs


def informe_split_unico(df: pd.DataFrame, *, test_ratio: float = 0.2) -> pd.DataFrame:
    """Tabla train vs test por estrategia (un solo corte temporal)."""

    train_df, test_df = dividir_train_test(df, test_ratio)
    m_train = metricas_por_estrategia_desde_ordenes(train_df)
    m_test = metricas_por_estrategia_desde_ordenes(test_df)
    estrategias = sorted(set(m_train) | set(m_test))
    rows: list[dict[str, float | int | str]] = []
    for e in estrategias:
        tr, te = m_train.get(e, {}), m_test.get(e, {})
        n_tr = int(tr.get("n", 0))
        n_te = int(te.get("n", 0))
        p_tr = float(tr.get("promedio", 0.0))
        p_te = float(te.get("promedio", 0.0))
        rows.append(
            {
                "estrategia": e,
                "n_train": n_tr,
                "retorno_medio_train": round(p_tr, 6),
                "winrate_train": round(float(tr.get("winrate", 0.0)), 4),
                "n_test": n_te,
                "retorno_medio_test": round(p_te, 6),
                "winrate_test": round(float(te.get("winrate", 0.0)), 4),
                "delta_retorno_test_train": round(p_te - p_tr, 6),
            }
        )
    return pd.DataFrame(rows).sort_values(by="delta_retorno_test_train", ascending=True)


def informe_walk_forward(df: pd.DataFrame, *, n_folds: int = 4) -> pd.DataFrame:
    """Agrega métricas de test por pliegue; útil para ver estabilidad OOS."""

    splits = walk_forward_splits(df, n_folds=n_folds)
    if not splits:
        return pd.DataFrame()

    from collections import defaultdict

    promedios_test: dict[str, list[float]] = defaultdict(list)
    ns_test: dict[str, list[int]] = defaultdict(list)

    for _train_df, test_df in splits:
        m_test = metricas_por_estrategia_desde_ordenes(test_df)
        for e, met in m_test.items():
            n_i = int(met.get("n", 0))
            if n_i <= 0:
                continue
            promedios_test[e].append(float(met["promedio"]))
            ns_test[e].append(n_i)

    rows = []
    for e in sorted(promedios_test.keys()):
        vals = promedios_test[e]
        ns = ns_test[e]
        rows.append(
            {
                "estrategia": e,
                "pliegues_con_presencia": len(vals),
                "retorno_medio_test_por_pliegue": round(sum(vals) / len(vals), 6),
                "std_retorno_test_entre_pliegues": round(
                    pd.Series(vals).std(ddof=0) if len(vals) > 1 else 0.0,
                    6,
                ),
                "n_total_test": int(sum(ns)),
            }
        )
    return pd.DataFrame(rows).sort_values(
        by="retorno_medio_test_por_pliegue", ascending=False
    )


def _iter_simbolos_desde_carpeta(carpeta: Path) -> Iterable[tuple[str, pd.DataFrame]]:
    if not carpeta.is_dir():
        return
    for path in sorted(carpeta.glob("*.parquet")):
        sym = symbol_desde_parquet_stem(path.stem)
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        yield sym, _ordenar_cronologico(df)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Informe OOS / walk-forward por estrategia (histórico de órdenes).",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Un solo símbolo (p. ej. BTC/USDT). Si se omite, se recorren *.parquet de la carpeta.",
    )
    parser.add_argument(
        "--carpeta",
        type=Path,
        default=None,
        help="Carpeta de parquet (por defecto CARPETA_ORDENES).",
    )
    parser.add_argument(
        "--walk-forward",
        action="store_true",
        help="Walk-forward por pliegues además del split único.",
    )
    parser.add_argument("--n-folds", type=int, default=4, help="Pliegues para walk-forward.")
    parser.add_argument(
        "--test-ratio",
        type=float,
        default=0.2,
        help="Fracción final reservada a test en el split único.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=10,
        help="Mínimo de filas para generar informe de un símbolo.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Prefijo CSV: <out>_<symbol>_split.csv y opcionalmente *_wf.csv",
    )
    args = parser.parse_args(argv)

    series: list[tuple[str, pd.DataFrame]] = []
    if args.symbol:
        try:
            hist = cargar_historial_operaciones(args.symbol)
        except (FileNotFoundError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
        series.append((args.symbol, _ordenar_cronologico(hist.data)))
    else:
        base = (args.carpeta or CARPETA_ORDENES).resolve()
        series = list(_iter_simbolos_desde_carpeta(base))
        if not series:
            print(f"Sin parquet en {base}", file=sys.stderr)
            return 1

    any_out = False
    for symbol, df in series:
        if len(df) < args.min_rows:
            continue
        any_out = True
        print(f"\n======== {symbol} (n={len(df)}) ========")
        split_df = informe_split_unico(df, test_ratio=args.test_ratio)
        print("--- Split único (train inicial | test final) ---")
        print(split_df.to_string(index=False))
        wf_df = pd.DataFrame()
        if args.walk_forward:
            wf_df = informe_walk_forward(df, n_folds=args.n_folds)
            print("\n--- Walk-forward (promedio retorno test por pliegue) ---")
            if wf_df.empty:
                print("(insuficientes datos para pliegues)")
            else:
                print(wf_df.to_string(index=False))

        if args.out:
            out = args.out.expanduser().resolve()
            stem = out.stem if out.suffix.lower() == ".csv" else out.name
            folder = out.parent
            folder.mkdir(parents=True, exist_ok=True)
            safe = symbol.replace("/", "_")
            split_df.to_csv(folder / f"{stem}_{safe}_split.csv", index=False)
            if args.walk_forward and not wf_df.empty:
                wf_df.to_csv(folder / f"{stem}_{safe}_wf.csv", index=False)

    if not any_out:
        print("Ningún símbolo cumple --min-rows.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
