"""Atribución de retornos por estrategia a partir de órdenes en Parquet.

Consolida todos los ``*.parquet`` de la carpeta de órdenes (la misma que
:class:`learning.historial_operaciones.CARPETA_ORDENES`: simulado o real según
``MODO_REAL`` / ``CARPETA_ORDENES_PATH``).

Uso:

    python -m core.diag.atribucion_estrategias
    python -m core.diag.atribucion_estrategias --dias 30
    python -m core.diag.atribucion_estrategias --out logs/atribucion_global.csv

Reutiliza la misma lógica que :func:`learning.analisis_resultados.analizar_estrategias_en_ordenes`
(distribución del retorno de cada orden entre ``estrategias_activas``).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from learning.analisis_resultados import analizar_estrategias_en_ordenes
from learning.historial_operaciones import CARPETA_ORDENES, symbol_desde_parquet_stem


def atribucion_desde_carpeta(
    carpeta: Path | str | None = None,
    *,
    dias: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve ``(global_por_estrategia, detalle_por_simbolo)``.

    - **global**: suma de métricas de todas las órdenes de todos los símbolos.
    - **detalle**: mismas columnas que por archivo, más ``symbol``, una fila por
      estrategia y símbolo (útil para ver dónde aporta cada nombre).
    """
    base = Path(carpeta) if carpeta is not None else CARPETA_ORDENES
    base = base.resolve()
    if not base.is_dir():
        vacío = pd.DataFrame()
        return vacío, vacío

    frames: list[pd.DataFrame] = []
    for path in sorted(base.glob("*.parquet")):
        symbol = symbol_desde_parquet_stem(path.stem)
        df = analizar_estrategias_en_ordenes(str(path), dias=dias)
        if df.empty:
            continue
        part = df.copy()
        part["symbol"] = symbol
        frames.append(part)

    if not frames:
        vacío = pd.DataFrame()
        return vacío, vacío

    detalle = pd.concat(frames, ignore_index=True)
    detalle = detalle.sort_values(
        by=["symbol", "retorno_total"],
        ascending=[True, False],
    )

    agg = (
        detalle.groupby("estrategia", as_index=False)
        .agg(
            ganadas=("ganadas", "sum"),
            perdidas=("perdidas", "sum"),
            total=("total", "sum"),
            retorno_total=("retorno_total", "sum"),
        )
    )
    agg["winrate"] = (
        (agg["ganadas"] / agg["total"] * 100).where(agg["total"] > 0, 0.0).round(2)
    )
    agg["retorno_promedio"] = (
        (agg["retorno_total"] / agg["total"]).where(agg["total"] > 0, 0.0).round(5)
    )
    agg = agg.sort_values(by="retorno_total", ascending=False)
    return agg, detalle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Informe de atribución por estrategia desde Parquet de órdenes.",
    )
    parser.add_argument(
        "--carpeta",
        type=Path,
        default=None,
        help="Carpeta con *.parquet (por defecto CARPETA_ORDENES del historial).",
    )
    parser.add_argument(
        "--dias",
        type=int,
        default=None,
        help="Solo órdenes con timestamp en los últimos N días (UTC).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Prefijo de exportación: escribe <out>_global.csv y <out>_por_simbolo.csv",
    )
    parser.add_argument(
        "--solo-global",
        action="store_true",
        help="Solo imprime la tabla global (no el detalle por símbolo).",
    )
    args = parser.parse_args(argv)

    global_df, detalle_df = atribucion_desde_carpeta(args.carpeta, dias=args.dias)

    if global_df.empty:
        print(f"Sin datos en {args.carpeta or CARPETA_ORDENES}", file=sys.stderr)
        return 1

    print("=== Atribución global (todas las órdenes / símbolos) ===")
    print(global_df.to_string(index=False))
    if not args.solo_global and not detalle_df.empty:
        print("\n=== Por símbolo (estrategia × par) ===")
        print(detalle_df.to_string(index=False))

    if args.out:
        out = args.out.expanduser().resolve()
        if out.suffix.lower() == ".csv":
            stem, folder = out.stem, out.parent
        else:
            stem, folder = out.name, out.parent
        folder.mkdir(parents=True, exist_ok=True)
        g_path = folder / f"{stem}_global.csv"
        d_path = folder / f"{stem}_por_simbolo.csv"
        global_df.to_csv(g_path, index=False)
        detalle_df.to_csv(d_path, index=False)
        print(f"\nCSV: {g_path}\n     {d_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
