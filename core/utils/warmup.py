from __future__ import annotations
from typing import List, Tuple, Dict
from core.utils.utils import intervalo_a_segundos

FRAME_CACHE: Dict[str, int] = {}

def _frame_ms(intervalo: str) -> int:
    if intervalo not in FRAME_CACHE:
        FRAME_CACHE[intervalo] = intervalo_a_segundos(intervalo) * 1000
    return FRAME_CACHE[intervalo]


def is_aligned(ts: int, intervalo: str) -> bool:
    """Return True if ``ts`` is multiple of frame length for ``intervalo``."""
    return ts % _frame_ms(intervalo) == 0


def check_warmup_contiguity(candles: List[Tuple[int, dict]], intervalo: str) -> None:
    """Ensure candles are aligned and contiguous for ``intervalo``.

    Raises ``ValueError`` if alignment or continuity is broken.
    """
    if not candles:
        return
    frame = _frame_ms(intervalo)
    prev = candles[0][0]
    if not is_aligned(prev, intervalo):
        raise ValueError(f"Timestamp {prev} no alineado a {frame}ms")
    for curr, _ in candles[1:]:
        if not is_aligned(curr, intervalo):
            raise ValueError(f"Timestamp {curr} no alineado a {frame}ms")
        expected = prev + frame
        if curr != expected:
            raise ValueError(f"Hueco entre {prev} y {curr}")
        prev = curr


def splice_with_last(
    warmup: List[Tuple[int, dict]],
    last_ts: int | None,
    last_candle: dict | None,
    intervalo: str,
) -> List[Tuple[int, dict]]:
    """Validate ``warmup`` and append ``last_candle`` if needed.

    ``last_ts`` must match ``last_candle['timestamp']`` when both are provided.
    Returns the validated and optionally extended sequence.
    """
    frame = _frame_ms(intervalo)
    check_warmup_contiguity(warmup, intervalo)
    if last_ts is not None:
        if not is_aligned(last_ts, intervalo):
            raise ValueError(f"Timestamp {last_ts} no alineado a {frame}ms")
        if warmup:
            if warmup[-1][0] != last_ts:
                if not last_candle or last_candle.get("timestamp") != last_ts:
                    raise ValueError("Última vela no coincide con timestamp dado")
                warmup.append((last_ts, last_candle))
        else:
            if not last_candle:
                raise ValueError("No hay warmup y falta última vela")
            if last_candle.get("timestamp") != last_ts:
                raise ValueError("Última vela no coincide con timestamp dado")
            warmup.append((last_ts, last_candle))
    return warmup