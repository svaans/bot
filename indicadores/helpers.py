"""Helpers y utilidades comunes para indicadores técnicos.

Este módulo incluye funciones de normalización, filtros y un mecanismo de
cacheo ligero y *thread-safe* para evitar recalcular indicadores sobre el mismo
``DataFrame``. El cache se implementa usando ``DataFrame.attrs`` pero envuelve
los valores en una clase con una comparación segura para evitar errores como
``ValueError: The truth value of a Series is ambiguous`` cuando Pandas intenta
comparar atributos que contienen ``Series``. Además, el cache se limita usando
un algoritmo LRU configurable para evitar consumo descontrolado de memoria.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Hashable

import pandas as pd

from .settings import get_indicator_settings
from indicadores.momentum import calcular_momentum
from indicadores.atr import calcular_atr
from indicadores.slope import calcular_slope
from indicadores.volumen import calcular_volumen_normalizado
from indicadores.volatilidad import calcular_volatilidad_normalizada


def filtrar_cerradas(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve solo las filas con velas cerradas.

    Si la columna ``is_closed`` no está presente, se devuelve el ``DataFrame``
    original sin modificar.
    """
    if 'is_closed' in df.columns:
        return df[df['is_closed']]
    return df


def serie_cierres(data) -> pd.Series:
    """Obtiene la serie de cierres usando solo velas cerradas."""
    if isinstance(data, pd.DataFrame):
        return filtrar_cerradas(data)['close']
    return data


def sanitize_series(
    serie: pd.Series,
    *,
    sort: bool = True,
    fill_method: str = 'ffill',
    clip_min: float | None = None,
    clip_max: float | None = None,
    normalize: bool | None = None,
) -> pd.Series:
    """Normaliza y limpia una serie numérica para garantizar determinismo.

    - Convierte a ``float`` y opcionalmente ordena por índice para evitar
      efectos de reordenamiento.
    - Rellena huecos según ``fill_method`` (``ffill`` por defecto) para manejar
      gaps de datos.
    - Si ``normalize`` es ``True`` aplica una normalización min-max. Cuando es
      ``None`` se toma el valor por defecto desde la configuración global de
      indicadores.
    - Opcionalmente clipea los valores entre ``clip_min`` y ``clip_max``.
    """
    serie = serie.astype(float)
    if sort:
        serie = serie.sort_index()
    if fill_method == 'ffill':
        serie = serie.ffill().bfill()
    elif fill_method == 'interpolate':
        serie = serie.interpolate().ffill().bfill()
    if normalize is None:
        normalize = get_indicator_settings().sanitize_normalize_default
    if normalize and not serie.empty:
        minimo = serie.min()
        maximo = serie.max()
        rango = maximo - minimo
        if rango != 0:
            serie = (serie - minimo) / rango
        else:
            serie = serie - minimo
    if clip_min is not None or clip_max is not None:
        serie = serie.clip(lower=clip_min, upper=clip_max)
    return serie


@dataclass
class _CacheEntry:
    """Contenedor ligero para valores de indicadores en cache.

    Define ``__eq__`` para evitar que Pandas intente comparar internamente
    ``Series`` o ``DataFrames`` almacenados en ``attrs``, lo que provocaría
    errores de verdad ambigua."""

    value: Any

    def __eq__(self, other: object) -> bool:  # pragma: no cover - trivial
        return self is other


class _IndicatorsCache:
    """Cache LRU *thread-safe* para indicadores asociados a un ``DataFrame``."""

    __slots__ = ('max_entries', '_store', '_lock')

    def __init__(self, max_entries: int) -> None:
        self.max_entries = max_entries
        self._store: 'OrderedDict[tuple[Hashable, ...], _CacheEntry]' = OrderedDict()
        self._lock = RLock()

    def get_or_compute(
        self,
        key: tuple[Hashable, ...],
        compute: Callable[[], Any],
    ) -> Any:
        """Obtiene un valor del cache y mantiene la política LRU."""

        with self._lock:
            entry = self._store.get(key)
            if entry is not None:
                self._store.move_to_end(key)
                return entry.value
            if self.max_entries <= 0:
                return compute()

            value = compute()

            wrapped = _CacheEntry(value)
            self._store[key] = wrapped
            self._store.move_to_end(key)
            while len(self._store) > self.max_entries:
                self._store.popitem(last=False)
            return wrapped.value

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def resize(self, max_entries: int) -> None:
        with self._lock:
            self.max_entries = max_entries
            if max_entries <= 0:
                self._store.clear()
                return
            while len(self._store) > max_entries:
                self._store.popitem(last=False)

    def set(self, key: tuple[Hashable, ...], value: Any) -> Any:
        """Inserta ``value`` en cache respetando la política LRU."""

        with self._lock:
            if self.max_entries <= 0:
                return value

            wrapped = _CacheEntry(value)
            self._store[key] = wrapped
            self._store.move_to_end(key)
            while len(self._store) > self.max_entries:
                self._store.popitem(last=False)
            return wrapped.value

    def __getstate__(self) -> dict[str, Any]:  # pragma: no cover - comportamiento pandas
        """Evita copiar la estructura interna cuando Pandas duplica ``attrs``."""

        return {'max_entries': self.max_entries}

    def __setstate__(self, state: dict[str, Any]) -> None:  # pragma: no cover - comportamiento pandas
        self.max_entries = int(state.get('max_entries', 0))
        self._store = OrderedDict()
        self._lock = RLock()


def _ensure_cache(df: pd.DataFrame) -> _IndicatorsCache:
    settings = get_indicator_settings()
    cache = df.attrs.get('_indicators_cache')
    if not isinstance(cache, _IndicatorsCache):
        cache = _IndicatorsCache(settings.cache_max_entries)
        df.attrs['_indicators_cache'] = cache
    else:
        if cache.max_entries != settings.cache_max_entries:
            cache.resize(settings.cache_max_entries)
    return cache


def _cached_value(
    df: pd.DataFrame, key: tuple[Hashable, ...], compute: Callable[[pd.DataFrame], Any]
) -> Any:
    """Obtiene un valor en cache asociado al ``DataFrame``."""

    settings = get_indicator_settings()
    if settings.cache_max_entries <= 0:
        return compute(df)

    cache = _ensure_cache(df)
    return cache.get_or_compute(key, lambda: compute(df))


def clear_cache(df: pd.DataFrame) -> None:
    """Elimina el cache de indicadores asociado al DataFrame."""
    cache = df.attrs.pop('_indicators_cache', None)
    if isinstance(cache, _IndicatorsCache):
        cache.clear()


def set_cached_value(df: pd.DataFrame, key: tuple[Hashable, ...], value: Any) -> Any:
    """Escribe ``value`` en el cache del ``DataFrame`` si está habilitado."""

    settings = get_indicator_settings()
    if settings.cache_max_entries <= 0:
        return value

    cache = _ensure_cache(df)
    return cache.set(key, value)


def get_rsi(data, periodo: int = 14, serie_completa: bool = False):
    """Obtiene el RSI con soporte para ``DataFrame`` o ``Series``."""
    from indicadores.rsi import calcular_rsi
    if isinstance(data, pd.Series):
        return calcular_rsi(data, periodo, serie_completa)
    key = ('rsi', periodo, serie_completa)
    return _cached_value(data, key, lambda d: calcular_rsi(d, periodo, serie_completa))

def get_momentum(df: pd.DataFrame, periodo: int = 10):
    """Obtiene el momentum con cache simple."""
    key = ('momentum', periodo)
    return _cached_value(df, key, lambda d: calcular_momentum(d, periodo))


def resolve_momentum_threshold(symbol: str | None = None) -> float:
    """Resuelve el umbral mínimo de activación para la señal de momentum."""

    settings = get_indicator_settings()
    threshold = max(float(getattr(settings, "momentum_activation_threshold", 0.0)), 0.0)
    if not symbol:
        return threshold

    overrides = getattr(settings, "momentum_threshold_overrides", {})
    if isinstance(overrides, dict):
        override = overrides.get(str(symbol).upper())
        if override is not None:
            try:
                return max(float(override), 0.0)
            except (TypeError, ValueError):
                return threshold
    return threshold


def get_atr(df: pd.DataFrame, periodo: int = 14):
    """Obtiene el ATR con cache simple."""
    key = ('atr', periodo)
    return _cached_value(df, key, lambda d: calcular_atr(d, periodo))


def get_slope(df: pd.DataFrame, periodo: int = 5):
    """Obtiene la pendiente (slope) con cache simple."""
    key = ('slope', periodo)
    return _cached_value(df, key, lambda d: calcular_slope(d, periodo))


def get_volumen_norm(df: pd.DataFrame, ventana: int = 20):
    """Obtiene el volumen normalizado con cache simple."""
    key = ('volumen_norm', ventana)
    return _cached_value(df, key, lambda d: calcular_volumen_normalizado(d, ventana))


def get_volatility_norm(df: pd.DataFrame, ventana: int = 14):
    """Obtiene la volatilidad normalizada con cache simple."""
    key = ('volatility_norm', ventana)
    return _cached_value(
        df, key, lambda d: calcular_volatilidad_normalizada(d, ventana)
    )
