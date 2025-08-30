import os
import asyncio
from pathlib import Path
from typing import Dict, Tuple, Optional, Set
import pandas as pd
from binance_api.cliente import fetch_ohlcv_async
from core.metrics import registrar_warmup_progress
from core.utils.utils import configurar_logger

log = configurar_logger('bootstrap')

MIN_BARS = int(os.getenv('MIN_BARS', '200'))
CACHE_TTL = int(os.getenv('WARMUP_CACHE_TTL', '300'))

_cache_dir = Path('estado/cache')
_cache_dir.mkdir(parents=True, exist_ok=True)

_config: Dict[str, Tuple[str, any]] = {}
_pending_fetch: Set[str] = set()
_warned: Set[str] = set()
_progress: Dict[str, float] = {}


def _cache_path(symbol: str, tf: str) -> Path:
    sanitized = symbol.replace('/', '_')
    return _cache_dir / f"{sanitized}_{tf}.csv"


def _update_progress(symbol: str, count: int) -> None:
    progress = min(1.0, count / MIN_BARS)
    _progress[symbol] = progress
    registrar_warmup_progress(symbol, progress)


def get_progress(symbol: str) -> float:
    return _progress.get(symbol, 0.0)


def update_progress(symbol: str, count: int) -> None:
    _update_progress(symbol, count)


def reset_state() -> None:
    _pending_fetch.clear()
    _warned.clear()
    _progress.clear()


def enqueue_fetch(symbol: str) -> None:
    _pending_fetch.add(symbol)


def pending_symbols() -> Set[str]:
    return set(_pending_fetch)


async def warmup_symbol(symbol: str, tf: str, cliente) -> pd.DataFrame:
    """Carga histórico mínimo para ``symbol`` usando caché cuando es válido."""
    _config[symbol] = (tf, cliente)
    path = _cache_path(symbol, tf)
    df: Optional[pd.DataFrame] = None
    if path.exists() and (asyncio.get_event_loop().time() - path.stat().st_mtime) < CACHE_TTL:
        try:
            df = pd.read_csv(path)
        except Exception:
            log.exception(f'Error leyendo caché {path}, ignorando')
    if df is None:
        try:
            ohlcv = await fetch_ohlcv_async(cliente, symbol, tf, limit=MIN_BARS)
        except Exception as e:
            log.warning(f'⚠️ Error obteniendo histórico de {symbol}: {e}')
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            try:
                df.to_csv(path, index=False)
            except Exception:
                log.exception(f'Error guardando caché {path}')
    _update_progress(symbol, len(df))
    return df.tail(MIN_BARS)


def mark_warned(symbol: str) -> None:
    _warned.add(symbol)


def was_warned(symbol: str) -> bool:
    return symbol in _warned