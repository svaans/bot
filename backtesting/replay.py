from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from backtesting.isolation import reset_umbral_adaptativo_runtime
from core.strategies.strategy_engine import StrategyEngine


def load_ohlcv_csv(path: Path | str) -> pd.DataFrame:
    """Carga CSV tipo ``estado/cache/BTC_EUR_5m.csv`` (timestamp, open, high, low, close, volume)."""

    p = Path(path)
    df = pd.read_csv(p)
    need = {"open", "high", "low", "close", "volume"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas OHLCV en {p}: {sorted(missing)}")
    return df


async def replay_entradas_async(
    symbol: str,
    df: pd.DataFrame,
    *,
    window: int,
    step: int,
    config: Mapping[str, Any],
    engine: StrategyEngine | None = None,
    reset_umbral_state: bool = True,
) -> list[dict[str, Any]]:
    """Evalúa la compuerta de entrada en ventanas rodantes (mismo criterio que en vivo).

    Parameters
    ----------
    window
        Número de velas en cada ventana pasada a :meth:`StrategyEngine.evaluar_entrada`.
    step
        Avance entre evaluaciones (1 = cada cierre).
    reset_umbral_state
        Si True, limpia el estado del umbral adaptativo antes del replay (recomendado).
    """

    if window < 5:
        raise ValueError("window debe ser >= 5 para indicadores razonables")
    if step < 1:
        raise ValueError("step debe ser >= 1")
    n = len(df)
    if n < window:
        return []

    if reset_umbral_state:
        reset_umbral_adaptativo_runtime()

    eng = engine or StrategyEngine()
    cfg = dict(config)
    rows: list[dict[str, Any]] = []

    for end in range(window, n + 1, step):
        wdf = df.iloc[end - window : end].copy()
        out = await eng.evaluar_entrada(symbol, wdf, config=cfg)
        out = dict(out)
        out["bar_end_index"] = end - 1
        rows.append(out)
    return rows


def summarize_replay(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Agrega métricas legibles para comparar configuraciones."""

    if not rows:
        return {
            "evaluaciones": 0,
            "permitido_count": 0,
            "permitido_rate": 0.0,
            "motivos_rechazo": {},
        }

    permitidos = sum(1 for r in rows if r.get("permitido"))
    motivos: Counter[str] = Counter()
    for r in rows:
        if not r.get("permitido"):
            m = r.get("motivo_rechazo")
            motivos[str(m or "sin_motivo")] += 1

    scores = [float(r.get("score_total") or 0.0) for r in rows]
    st = [float(r.get("score_tecnico") or 0.0) for r in rows]

    return {
        "evaluaciones": len(rows),
        "permitido_count": permitidos,
        "permitido_rate": round(permitidos / len(rows), 4),
        "motivos_rechazo": dict(motivos),
        "score_total_mean": round(sum(scores) / len(scores), 4),
        "score_tecnico_mean": round(sum(st) / len(st), 4),
    }
