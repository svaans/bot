import hashlib
import os
import time
from typing import Dict, Tuple

# Ventana de deduplicación en segundos
_DEDUP_WINDOW = int(os.getenv("ORDER_DEDUP_WINDOW", "60"))

# Historial de ordenes enviadas {cid: timestamp}
_SENT_CIDS: Dict[str, float] = {}
# Mapa para deduplicación por (symbol, side, candle_close_ts)
_SEEN_KEYS: Dict[Tuple[str, str, int], str] = {}


def _purge_expired(now: float) -> None:
    """Elimina CIDs fuera de la ventana de deduplicación."""
    expirados = [cid for cid, ts in _SENT_CIDS.items() if now - ts >= _DEDUP_WINDOW]
    for cid in expirados:
        del _SENT_CIDS[cid]
        for key, value in list(_SEEN_KEYS.items()):
            if value == cid:
                del _SEEN_KEYS[key]


def _make_client_order_id(symbol: str, side: str, candle_close_ts: int, version: str) -> str:
    """Genera un ID determinista para la orden."""
    base = f"{symbol}|{side}|{candle_close_ts}|{version}"
    return hashlib.sha256(base.encode()).hexdigest()


def place_order(
    symbol: str,
    side: str,
    candle_close_ts: int,
    strategy_version: str,
    *,
    now: float | None = None,
) -> dict:
    """Registra una orden si no es un duplicado reciente."""
    current = now if now is not None else time.time()
    _purge_expired(current)
    cid = _make_client_order_id(symbol, side, candle_close_ts, strategy_version)
    key = (symbol, side, candle_close_ts)

    # Duplicado por CID en ventana Δt
    ts = _SENT_CIDS.get(cid)
    if ts and current - ts < _DEDUP_WINDOW:
        return {"status": "SKIP_DUPLICATE", "client_order_id": cid}

    # Deduplicación por (symbol, side, candle_close_ts)
    existing = _SEEN_KEYS.get(key)
    if existing and current - _SENT_CIDS.get(existing, 0) < _DEDUP_WINDOW:
        return {"status": "SKIP_DUPLICATE", "client_order_id": existing}

    _SENT_CIDS[cid] = current
    _SEEN_KEYS[key] = cid
    return {"status": "SENT", "client_order_id": cid}