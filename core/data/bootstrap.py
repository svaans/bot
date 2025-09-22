import os
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, Set, Iterable, List
import pandas as pd
import numpy as np
from binance_api.cliente import fetch_ohlcv_async, obtener_cliente
from core.metrics import registrar_warmup_progress
from core.utils.utils import configurar_logger

log = configurar_logger('bootstrap')

MIN_BARS = int(os.getenv('MIN_BARS', '400'))
CACHE_TTL = int(os.getenv('WARMUP_CACHE_TTL', '300'))

_cache_dir = Path('estado/cache')
_cache_dir.mkdir(parents=True, exist_ok=True)

# symbol -> (tf, cliente, min_bars)
_config: Dict[str, Tuple[str, Any, int]] = {}
_pending_fetch: Set[str] = set()
_warned: Set[str] = set()
# Progreso por símbolo (0..1)
_progress: Dict[str, float] = {}


def _cache_path(symbol: str, tf: str) -> Path:
    sanitized = symbol.replace('/', '_').upper()
    return _cache_dir / f"{sanitized}_{tf}.csv"


def _update_progress(symbol: str, count: int, target: int = MIN_BARS) -> None:
    target = max(1, int(target))
    progress = min(1.0, max(0.0, count / target))
    _progress[symbol] = progress
    try:
        registrar_warmup_progress(symbol, progress)
    except Exception:
        # Métricas opcionales; no debe romper el warmup
        pass


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




def _coerce_numeric(serie: pd.Series, *, dtype: Optional[str] = None) -> pd.Series:
    """Convierte valores a numéricos manejando infinitos y NaN de forma explícita."""

    numerica = pd.to_numeric(serie, errors='coerce')
    numerica = numerica.replace([np.inf, -np.inf], np.nan)
    if dtype is not None:
        numerica = numerica.astype(dtype)
    return numerica


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columnas esperadas, orden y tipos básicos."""
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    # Asegurar columnas
    df = df.reindex(columns=cols, fill_value=None)
    # Coerciones seguras
    df['timestamp'] = _coerce_numeric(df['timestamp'], dtype='Int64')
    for c in ['open', 'high', 'low', 'close', 'volume']:
        df[c] = _coerce_numeric(df[c])
    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close']).copy()
    # Asegurar orden ascendente y sin duplicados
    df = df.sort_values('timestamp', kind='mergesort')
    df = df.drop_duplicates(subset=['timestamp'], keep='last')
    # Convertir timestamp a int64 nativo si es posible
    if pd.api.types.is_integer_dtype(df['timestamp']):
        df['timestamp'] = df['timestamp'].astype('int64')
    return df


async def warmup_symbol(
    symbol: str, tf: str, cliente, min_bars: int = MIN_BARS
) -> Optional[pd.DataFrame]:
    """Carga ``min_bars`` de histórico para ``symbol`` usando caché cuando es válida.

    Devuelve un DataFrame normalizado (ascendente por timestamp) con exactamente
    ``min_bars`` filas si fue posible, o ``None`` si no se alcanzó el mínimo.
    """
    symbol_u = symbol.upper()
    _config[symbol_u] = (tf, cliente, int(min_bars))

    path = _cache_path(symbol_u, tf)
    df: Optional[pd.DataFrame] = None

    # Intentar usar caché si es reciente
    if path.exists() and (time.time() - path.stat().st_mtime) < CACHE_TTL:
        try:
            tmp = pd.read_csv(
                path,
                dtype={
                    'timestamp': 'int64',
                    'open': 'float64',
                    'high': 'float64',
                    'low': 'float64',
                    'close': 'float64',
                    'volume': 'float64',
                },
            )
            tmp = _normalize_df(tmp)
            if len(tmp) >= min_bars:
                df = tmp
        except Exception:
            log.exception(f'Error leyendo caché {path}, ignorando')

    # Si no hay caché válida, pedir a la API
    if df is None:
        try:
            ohlcv = await fetch_ohlcv_async(cliente, symbol_u, tf, limit=int(min_bars))
        except Exception as e:
            log.warning(f'⚠️ Error obteniendo histórico de {symbol_u}: {e}')
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
            )
            df = _normalize_df(df)
            # Persistir caché (best-effort)
            try:
                df.to_csv(path, index=False)
            except Exception:
                log.exception(f'Error guardando caché {path}')

    if df is None:
        df = pd.DataFrame()

    count = int(len(df))
    _update_progress(symbol_u, count, min_bars)

    if count < min_bars:
        log.error('❌ Warmup incompleto para %s: %s/%s velas disponibles', symbol_u, count, min_bars)
        return None

    # Mantener exactamente min_bars últimas velas
    return df.tail(int(min_bars)).copy()


async def warmup_inicial(
    symbols: Iterable[str], tf: str = "5m", min_bars: int | None = None
) -> None:
    """Calienta símbolos en paralelo y asegura progreso mínimo.

    - Convierte ``symbols`` a lista para evitar consumir iteradores dos veces.
    - Reintenta selectivamente símbolos con progreso < umbral.
    """
    symbols_list: List[str] = [s.upper() for s in symbols]
    try:
        cliente = obtener_cliente()
    except Exception as e:
        log.warning(f'⚠️ No se pudo obtener cliente: {e}')
        cliente = None

    target = int(min_bars if min_bars is not None else MIN_BARS)

    # Lanzar warmup en paralelo
    await asyncio.gather(*(warmup_symbol(s, tf, cliente, min_bars=target) for s in symbols_list))

    # Reintentos selectivos bajo umbral
    umbral = float(os.getenv('WARMUP_SUCCESS_THRESHOLD', '0.9'))
    retry = [s for s in symbols_list if get_progress(s) < umbral]
    for s in retry:
        await warmup_symbol(s, tf, cliente, min_bars=target)


def mark_warned(symbol: str) -> None:
    _warned.add(symbol.upper())


def was_warned(symbol: str) -> bool:
    return symbol.upper() in _warned

