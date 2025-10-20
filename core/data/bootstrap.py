import os
import time
import asyncio
import importlib
import sys
import math
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, Set, Iterable, List
import pandas as pd
import pandas as _PD_REAL
import numpy as np
from binance_api.cliente import fetch_ohlcv_async, obtener_cliente
from core.metrics import registrar_warmup_progress
from core.utils.io_metrics import observe_disk_write
from core.utils.io_metrics import observe_disk_write
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




def _ensure_real_pandas():
    """Garantiza que contamos con el módulo real de pandas."""

    global _PD_REAL
    ctor = getattr(_PD_REAL, 'DataFrame', None)
    if callable(ctor) and getattr(ctor, '__module__', '').startswith('pandas'):
        return _PD_REAL

    try:
        sys.modules.pop('pandas', None)
        importlib.invalidate_caches()
        real_pd = importlib.import_module('pandas')
        ctor = getattr(real_pd, 'DataFrame', None)
        if not (callable(ctor) and getattr(ctor, '__module__', '').startswith('pandas')):
            return None
    except Exception:
        return None

    _PD_REAL = real_pd
    return real_pd


def _normalize_df_stub(df: Any, cols: List[str]):
    """Normaliza estructuras tipo DataFrame sin depender de pandas real."""

    data = getattr(df, '_data', None)
    columns = list(getattr(df, '_columns', [])) or list(cols)
    if data is None:
        try:
            data = list(df)  # type: ignore[arg-type]
        except Exception:
            data = []

    rows: List[Dict[str, float]] = []
    for entry in data:
        mapping: Dict[str, Any]
        if isinstance(entry, dict):
            mapping = {str(k): entry.get(k) for k in columns}
        else:
            mapping = {}
            for idx, col in enumerate(columns):
                if isinstance(entry, (list, tuple)) and idx < len(entry):
                    mapping[col] = entry[idx]
        try:
            ts = int(float(mapping.get('timestamp', 0)))
            o = float(mapping.get('open', float('nan')))
            h = float(mapping.get('high', float('nan')))
            l = float(mapping.get('low', float('nan')))
            c = float(mapping.get('close', float('nan')))
            v = float(mapping.get('volume', float('nan')))
        except (TypeError, ValueError):
            continue
        if ts <= 0 or any(math.isnan(val) or math.isinf(val) for val in (o, h, l, c, v)):
            continue
        rows.append({'timestamp': ts, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v})

    rows.sort(key=lambda item: item['timestamp'])
    dedup: Dict[int, Dict[str, float]] = {}
    for row in rows:
        dedup[row['timestamp']] = row
    ordered = [dedup[k] for k in sorted(dedup)]

    normalized = [[row[col] for col in cols] for row in ordered]
    ctor = getattr(df, '__class__', None)
    if callable(ctor):
        try:
            return ctor(normalized, columns=cols)
        except Exception:
            pass

    class _MiniFallback:
        def __init__(self, data=None, columns=None) -> None:
            self._data = list(data or [])
            self._columns = list(columns or [])

        def __len__(self) -> int:
            return len(self._data)

        def to_csv(self, *args, **kwargs) -> None:
            return None

    return _MiniFallback(normalized, columns=cols)


def _coerce_numeric(serie: pd.Series, *, dtype: Optional[str] = None) -> pd.Series:
    """Convierte valores a numéricos manejando infinitos y NaN de forma explícita."""

    real_pd = _ensure_real_pandas()
    if real_pd is None:
        raise RuntimeError('Pandas no disponible para normalizar datos históricos')

    numerica = real_pd.to_numeric(serie, errors='coerce')
    numerica = numerica.replace([np.inf, -np.inf], np.nan)
    if dtype is not None:
        numerica = numerica.astype(dtype)
    return numerica


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columnas esperadas, orden y tipos básicos."""
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    real_pd = _ensure_real_pandas()
    if real_pd is None:
        return _normalize_df_stub(df, cols)

    # Los tests pueden inyectar stubs muy simples que no implementan la API
    # completa de pandas. Convertimos explícitamente a DataFrame real si es
    # necesario para operar con métodos como ``reindex``.
    if not hasattr(df, 'reindex') or not isinstance(df, real_pd.DataFrame):
        try:
            data = getattr(df, '_data', None)
            columns = getattr(df, '_columns', None)
            if data is None:
                # Intentar iterar sobre el objeto como fallback.
                data = list(df)  # type: ignore[arg-type]
            df = real_pd.DataFrame(data, columns=columns)
        except Exception:
            return real_pd.DataFrame(columns=cols)
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
    if real_pd.api.types.is_integer_dtype(df['timestamp']):
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
                observe_disk_write(
                    'warmup_cache_csv',
                    path,
                    lambda: df.to_csv(path, index=False),
                )
            except Exception:
                log.exception(f'Error guardando caché {path}')

    if df is None:
        df = pd.DataFrame()

    # Harden contra stubs sin __len__
    try:
        count = int(len(df))
    except Exception:
        count = 0

    _update_progress(symbol_u, count, min_bars)

    if count < min_bars:
        log.error('❌ Warmup incompleto para %s: %s/%s velas disponibles', symbol_u, count, min_bars)
        # Devolver un DF vacío coherente (o None)
        try:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        except Exception:
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


