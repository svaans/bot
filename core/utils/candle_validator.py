from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class CandleValidationResult:
    """Métricas de saneo de velas."""

    duplicates: int = 0
    gaps: int = 0
    misaligned: int = 0


def validate_5m_dataframe(
    df: pd.DataFrame, interval_ms: int = 300_000
) -> tuple[pd.DataFrame, CandleValidationResult]:
    """Sanea un ``DataFrame`` de velas de 5m.

    La función es idempotente: múltiples llamadas consecutivas sobre el mismo
    ``DataFrame`` no alteran el resultado después de la primera ejecución.

    Parameters
    ----------
    df : pd.DataFrame
        ``DataFrame`` con al menos una columna ``open_time`` (epoch ms).
    interval_ms : int, optional
        Intervalo esperado de las velas en milisegundos, por defecto ``300_000``.

    Returns
    -------
    tuple[pd.DataFrame, CandleValidationResult]
        ``DataFrame`` saneado e información sobre duplicados, gaps y timestamps
        desalineados detectados en la entrada original.
    """
    if df.empty:
        return df.copy(), CandleValidationResult()

    work = df.copy()
    work["open_time"] = work["open_time"].astype("int64")

    misaligned = int((work["open_time"] % interval_ms != 0).sum())
    work["open_time"] -= work["open_time"] % interval_ms

    before = len(work)
    work = work.drop_duplicates(subset="open_time", keep="first")
    duplicates = before - len(work)

    work = work.sort_values("open_time").set_index("open_time")

    start = int(work.index.min())
    end = int(work.index.max())
    full_index = pd.RangeIndex(start, end + interval_ms, interval_ms)
    gaps = full_index.size - work.index.size
    work = work.reindex(full_index)
    work.index.name = "open_time"

    result = CandleValidationResult(duplicates=duplicates, gaps=gaps, misaligned=misaligned)
    return work, result