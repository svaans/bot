from __future__ import annotations

import itertools
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from backtesting.replay import load_ohlcv_csv, replay_entradas_async, summarize_replay


def _normalize_grid(grid: Mapping[str, Sequence[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid.keys())
    combos: list[dict[str, Any]] = []
    for values in itertools.product(*(grid[k] for k in keys)):
        combos.append(dict(zip(keys, values, strict=True)))
    return combos


def merge_config(base: Mapping[str, Any] | None, overrides: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if base:
        out.update(base)
    out.update(overrides)
    return out


async def run_grid_search_async(
    csv_path: Path | str,
    symbol: str,
    *,
    window: int = 120,
    step: int = 5,
    base_config: Mapping[str, Any] | None = None,
    grid: Mapping[str, Sequence[Any]] | None = None,
) -> list[dict[str, Any]]:
    """Ejecuta varias configuraciones sobre el mismo CSV y devuelve resúmenes ordenables.

    Cada combinación en ``grid`` se fusiona con ``base_config`` y se corre un replay
    completo con estado de umbral adaptativo reiniciado (aislado).
    """

    df = load_ohlcv_csv(csv_path)
    combos = _normalize_grid(grid or {})
    results: list[dict[str, Any]] = []

    for overrides in combos:
        cfg = merge_config(base_config, overrides)
        rows = await replay_entradas_async(
            symbol,
            df,
            window=window,
            step=step,
            config=cfg,
            reset_umbral_state=True,
        )
        summary = summarize_replay(rows)
        results.append(
            {
                "config": cfg,
                "overrides": overrides,
                "summary": summary,
            }
        )
    return results


def sort_results_by_permitido_rate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        results,
        key=lambda r: (
            r["summary"]["permitido_rate"],
            r["summary"]["permitido_count"],
        ),
        reverse=True,
    )


def load_grid_json(path: Path | str) -> dict[str, list[Any]]:
    """JSON objeto: claves del dict ``config`` del motor → lista de valores."""

    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("El JSON de rejilla debe ser un objeto {clave: [valores...]}")
    out: dict[str, list[Any]] = {}
    for k, v in raw.items():
        if not isinstance(v, list):
            raise ValueError(f"Valores de rejilla para {k!r} deben ser lista, no {type(v)}")
        out[str(k)] = list(v)
    return out
