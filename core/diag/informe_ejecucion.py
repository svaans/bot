"""Resumen de slippage y calidad de fills desde el log JSONL (Fase 6).

Los registros los escribe :mod:`core.diag.execution_quality_log` al ejecutar
órdenes de mercado reales (:func:`core.orders.real_orders.ejecutar_orden_market`).

Ejemplos::

    python -m core.diag.informe_ejecucion
    python -m core.diag.informe_ejecucion --path logs/ejecuciones.jsonl --csv logs/slippage_por_simbolo.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from core.diag.execution_quality_log import execution_quality_log_path, load_ejecuciones_jsonl


def _resumen_por_simbolo(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for sym, sub in df.groupby("symbol", dropna=False):
        st = pd.to_numeric(sub["slippage_vs_ticker"], errors="coerce").dropna()
        row: dict = {
            "symbol": sym,
            "n": int(len(sub)),
            "mean_slip_ticker": float(st.mean()) if len(st) else float("nan"),
            "p95_slip_ticker": float(st.quantile(0.95)) if len(st) else float("nan"),
        }
        if "slippage_vs_senal" in sub.columns:
            ss = pd.to_numeric(sub["slippage_vs_senal"], errors="coerce").dropna()
            row["n_con_senal"] = int(len(ss))
            row["mean_slip_senal"] = float(ss.mean()) if len(ss) else float("nan")
            row["p95_slip_senal"] = float(ss.quantile(0.95)) if len(ss) else float("nan")
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("symbol").reset_index(drop=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resumen slippage por símbolo (log de ejecuciones).")
    parser.add_argument(
        "--path",
        type=Path,
        default=None,
        help="Ruta al .jsonl (default: EXECUTION_QUALITY_LOG_PATH o logs/ejecuciones.jsonl)",
    )
    parser.add_argument(
        "--side",
        choices=("buy", "sell"),
        default=None,
        help="Filtrar por lado (opcional).",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Si se indica, escribe el resumen en CSV.",
    )
    args = parser.parse_args(argv)

    path = args.path or execution_quality_log_path()
    records = load_ejecuciones_jsonl(path)
    if not records:
        print(f"Sin registros en {path}", file=sys.stderr)
        return 1

    df = pd.DataFrame(records)
    if args.side and "side" in df.columns:
        df = df[df["side"].astype(str).str.lower() == args.side].copy()

    if df.empty:
        print("Sin filas tras filtrar.", file=sys.stderr)
        return 1

    if "slippage_vs_ticker" not in df.columns:
        print("Los registros no incluyen slippage_vs_ticker.", file=sys.stderr)
        return 1

    res = _resumen_por_simbolo(df)
    pd.set_option("display.max_rows", 200)
    pd.set_option("display.width", 120)
    print(res.to_string(index=False, float_format=lambda x: f"{x:.6f}" if pd.notna(x) else ""))

    if args.csv is not None:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        res.to_csv(args.csv, index=False)
        print(f"Escrito {args.csv.resolve()}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
