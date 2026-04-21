"""CLI: python -m backtesting --help"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

_CLI_LOGGERS = (
    "config_manager",
    "loader_entradas",
    "entradas",
    "engine",
    "adaptador_umbral",
)


def _configure_cli_logging(*, verbose: bool) -> None:
    """Por defecto el backtest no spamea INFO (JSON de config/loader)."""

    if verbose:
        logging.basicConfig(level=logging.INFO, force=True)
        for name in _CLI_LOGGERS:
            logging.getLogger(name).setLevel(logging.INFO)
        return
    logging.basicConfig(level=logging.WARNING, force=True)
    for name in _CLI_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def _parse_list_floats(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def _parse_list_ints(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _parse_list_bools(s: str) -> list[bool]:
    out: list[bool] = []
    for x in s.split(","):
        t = x.strip().lower()
        if not t:
            continue
        if t in ("1", "true", "yes", "si", "sí"):
            out.append(True)
        elif t in ("0", "false", "no"):
            out.append(False)
        else:
            raise argparse.ArgumentTypeError(f"booleano no reconocido: {x!r}")
    return out


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Replay offline de la compuerta StrategyEngine sobre CSV OHLCV "
            "(p. ej. estado/cache/BTC_EUR_5m.csv) y rejilla de parámetros del dict config."
        )
    )
    p.add_argument("--csv", type=Path, required=True, help="Ruta al CSV OHLCV")
    p.add_argument("--symbol", type=str, default="BTC/EUR", help='Símbolo mercado, ej. "BTC/EUR"')
    p.add_argument("--window", type=int, default=120, help="Velas por ventana")
    p.add_argument("--step", type=int, default=5, help="Avance entre evaluaciones")
    p.add_argument(
        "--grid-json",
        type=Path,
        default=None,
        help='JSON {"umbral_score_tecnico": [1.5, 2.0], "diversidad_minima": [2, 3]}',
    )
    p.add_argument(
        "--umbral-score-tecnico-grid",
        type=str,
        default="",
        help="Lista separada por comas (alternativa a --grid-json)",
    )
    p.add_argument("--diversidad-grid", type=str, default="", help="Enteros separados por comas")
    p.add_argument(
        "--usar-score-tecnico-grid",
        type=str,
        default="",
        help="true/false separados por comas",
    )
    p.add_argument(
        "--out-json",
        type=Path,
        default=None,
        help="Si se indica, escribe resultados completos en este archivo",
    )
    p.add_argument("--top", type=int, default=10, help="Cuántas configs mostrar al ordenar por permitido_rate")
    p.add_argument(
        "--explain",
        action="store_true",
        help="Tras el ranking, muestra 3 evaluaciones de la mejor config (scores vs umbrales y motivo)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Muestra logs INFO del arranque (config, estrategias cargadas, etc.); por defecto están en WARNING",
    )
    return p


def _grid_from_args(args: argparse.Namespace) -> dict[str, list[Any]]:
    if args.grid_json is not None:
        from backtesting.grid_search import load_grid_json

        return load_grid_json(args.grid_json)

    grid: dict[str, list[Any]] = {}
    if args.umbral_score_tecnico_grid.strip():
        grid["umbral_score_tecnico"] = _parse_list_floats(args.umbral_score_tecnico_grid)
    if args.diversidad_grid.strip():
        grid["diversidad_minima"] = _parse_list_ints(args.diversidad_grid)
    if args.usar_score_tecnico_grid.strip():
        grid["usar_score_tecnico"] = _parse_list_bools(args.usar_score_tecnico_grid)
    return grid


async def _async_main(args: argparse.Namespace) -> int:
    from backtesting.grid_search import run_grid_search_async, sort_results_by_permitido_rate

    grid = _grid_from_args(args)
    if not grid:
        grid = {
            "umbral_score_tecnico": [1.5, 2.0, 2.5],
            "diversidad_minima": [2, 3],
        }

    results = await run_grid_search_async(
        args.csv,
        args.symbol,
        window=args.window,
        step=args.step,
        base_config=None,
        grid=grid,
    )
    ordered = sort_results_by_permitido_rate(results)

    for i, row in enumerate(ordered[: max(1, args.top)], start=1):
        ov = row["overrides"] or row["config"]
        sm = row["summary"]
        print(
            f"{i:2d}  permitido_rate={sm['permitido_rate']!s}  "
            f"permitidos={sm['permitido_count']}/{sm['evaluaciones']}  "
            f"overrides={ov!s}"
        )

    best = ordered[0]
    sm0 = best["summary"]
    print()
    print("--- Rechazos agregados (config #1 del ranking) ---")
    print(f"motivos_rechazo: {sm0['motivos_rechazo']}")
    print(
        f"medias: score_total={sm0.get('score_total_mean')}  "
        f"score_tecnico={sm0.get('score_tecnico_mean')}"
    )
    mr = sm0.get("motivos_rechazo") or {}
    if mr.get("validaciones_fallidas", 0) >= sm0.get("evaluaciones", 0) // 2:
        print(
            "Nota: la mayoría son validaciones_fallidas (RSI/slope/Bollinger cuando "
            "usar_score_tecnico=true). Para aislar umbrales vs señales, prueba en la rejilla "
            "`--usar-score-tecnico-grid false` (solo valida volumen entre las validaciones técnicas)."
        )
    if sm0.get("permitido_rate", 0) == 0 and not args.explain:
        print(
            "\n(Nada permitido: revisa motivos arriba. Si sigue poco claro, usa --explain. "
            "Causas habituales: score_bajo, validaciones_fallidas, diversidad_baja, score_tecnico_bajo.)"
        )

    if args.explain:
        from backtesting.replay import load_ohlcv_csv, replay_entradas_async

        df = load_ohlcv_csv(args.csv)
        rows = await replay_entradas_async(
            args.symbol,
            df,
            window=args.window,
            step=args.step,
            config=best["config"],
            reset_umbral_state=True,
        )
        print()
        print("--- Muestra (3 primeras evaluaciones, mejor config) ---")
        for r in rows[:3]:
            print(
                f"  bar={r.get('bar_end_index')}  permitido={r.get('permitido')}  "
                f"motivo={r.get('motivo_rechazo')}  tendencia={r.get('tendencia')}  "
                f"score_total={r.get('score_total')} umbral={r.get('umbral')!s}  "
                f"score_tec={r.get('score_tecnico')} umbral_tec={r.get('umbral_score_tecnico')}  "
                f"div={r.get('diversidad')}  val_fallidas={r.get('validaciones_fallidas')}"
            )

    if args.out_json:
        serializable = []
        for row in ordered:
            serializable.append(
                {
                    "overrides": row["overrides"],
                    "config": row["config"],
                    "summary": row["summary"],
                }
            )
        args.out_json.write_text(json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Escrito: {args.out_json}", file=sys.stderr)

    return 0


def main() -> None:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--verbose", action="store_true")
    pre_args, _ = pre.parse_known_args()
    _configure_cli_logging(verbose=bool(pre_args.verbose))

    parser = _build_arg_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()
