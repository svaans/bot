import os
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, Set, Iterable
import pandas as pd
from binance_api.cliente import fetch_ohlcv_async, obtener_cliente
from core.metrics import registrar_warmup_progress
from core.utils.utils import configurar_logger

log = configurar_logger('bootstrap')

MIN_BARS = int(os.getenv('MIN_BARS', '400'))
CACHE_TTL = int(os.getenv('WARMUP_CACHE_TTL', '300'))

_cache_dir = Path('estado/cache')
_cache_dir.mkdir(parents=True, exist_ok=True)

_config: Dict[str, Tuple[str, Any, int]] = {}
_pending_fetch: Set[str] = set()
_warned: Set[str] = set()
_progress: Dict[str, float] = {}


def _cache_path(symbol: str, tf: str) -> Path:
    sanitized = symbol.replace('/', '_')
    return _cache_dir / f"{sanitized}_{tf}.csv"


def _update_progress(symbol: str, count: int, target: int = MIN_BARS) -> None:
    progress = min(1.0, count / target)
    _progress[symbol] = progress
    registrar_warmup_progress(symbol, progress)


def get_progress(symbol: str) -> float:
    return _progress.get(symbol, 0.0)


def update_progress(symbol: str, count: int, target: int = MIN_BARS) -> None:
    _update_progress(symbol, count, target)


def reset_state() -> None:
    _pending_fetch.clear()
    _warned.clear()
    _progress.clear()


def enqueue_fetch(symbol: str) -> None:
    _pending_fetch.add(symbol)


def pending_symbols() -> Set[str]:
    return set(_pending_fetch)


async def warmup_symbol(symbol: str, tf: str, cliente, min_bars: int = MIN_BARS) -> pd.DataFrame:
    """Carga ``min_bars`` de histórico para ``symbol`` usando caché cuando es válido.

    Si la caché contiene menos velas de las requeridas, se ignora y se solicita
    nuevamente el histórico completo. De esta forma nos aseguramos de que el
    parámetro ``min_bars`` tenga efecto aun cuando existan archivos previos.
    """
    _config[symbol] = (tf, cliente, min_bars)
    path = _cache_path(symbol, tf)
    df: Optional[pd.DataFrame] = None
    if path.exists() and (time.time() - path.stat().st_mtime) < CACHE_TTL:
        try:
            tmp = pd.read_csv(
                path,
                dtype={
                    'timestamp': 'int64',
                    'open': 'float',
                    'high': 'float',
                    'low': 'float',
                    'close': 'float',
                    'volume': 'float',
                },
            )
            if len(tmp) >= min_bars:
                df = tmp
        except Exception:
            log.exception(f'Error leyendo caché {path}, ignorando')
    if df is None:
        try:
            ohlcv = await fetch_ohlcv_async(cliente, symbol, tf, limit=min_bars)
        except Exception as e:
            log.warning(f'⚠️ Error obteniendo histórico de {symbol}: {e}')
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            try:
                df.to_csv(path, index=False)
            except Exception:
                log.exception(f'Error guardando caché {path}')
    _update_progress(symbol, len(df), min_bars)
    return df.tail(min_bars)


async def warmup_inicial(
    symbols: Iterable[str], tf: str = "5m", min_bars: int | None = None
) -> None:
    """Calienta los ``symbols`` y bloquea hasta completar el warmup.

    Se lanza al arrancar para evitar advertencias de datos insuficientes antes
    de evaluar estrategias. Si algún símbolo no alcanza el total requerido de
    velas, se reintenta hasta conseguir ``MIN_BARS``.
    """

    cliente = obtener_cliente()
    target = min_bars if min_bars is not None else MIN_BARS
    pendientes = list(symbols)
    while pendientes:
        await asyncio.gather(
            *(warmup_symbol(s, tf, cliente, min_bars=target) for s in pendientes)
        )
        pendientes = [s for s in pendientes if get_progress(s) < 1.0]
        if pendientes:
            await asyncio.sleep(0.5)


def mark_warned(symbol: str) -> None:
    _warned.add(symbol)


def was_warned(symbol: str) -> bool:
    return symbol in _warned
