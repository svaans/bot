"""Compatibilidad con Prometheus para entornos opcionales.

Este módulo centraliza el acceso a métricas de :mod:`prometheus_client` y
proporciona implementaciones nulas cuando la librería no está disponible. De
esta forma evitamos fallos en tiempo de importación y mantenemos una interfaz
uniforme para el resto del código del bot.
"""

from __future__ import annotations

import importlib
from contextlib import suppress
from typing import Any, Callable, ContextManager, Protocol, Self

__all__ = [
    "Counter",
    "Gauge",
    "Histogram",
    "Metric",
    "MetricWrapperBase",
    "REGISTRY",
    "HAVE_PROM",
    "start_wsgi_server",
]


class Metric(Protocol):
    """Interfaz mínima soportada por los contadores de Prometheus."""

    def labels(self, *args: Any, **kwargs: Any) -> Self:  # pragma: no cover - interfaz
        ...

    def inc(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - interfaz
        ...

    def set(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - interfaz
        ...

    def observe(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - interfaz
        ...

    def time(self, *args: Any, **kwargs: Any) -> ContextManager[Any]:  # pragma: no cover
        ...

    def count_exceptions(self, *args: Any, **kwargs: Any) -> ContextManager[Any]:  # pragma: no cover
        ...


class _NullContextManager(ContextManager[None]):
    """Contexto vacío utilizado por las métricas nulas."""

    def __enter__(self) -> None:  # pragma: no cover - comportamiento trivial
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:  # pragma: no cover - comportamiento trivial
        return False


class _NullMetric:
    """Implementación nula que replica la interfaz básica de Prometheus."""

    def labels(self, *args: Any, **kwargs: Any) -> "_NullMetric":
        return self

    def inc(self, *args: Any, **kwargs: Any) -> None:
        return None

    def set(self, *args: Any, **kwargs: Any) -> None:
        return None

    def observe(self, *args: Any, **kwargs: Any) -> None:
        return None

    def time(self, *args: Any, **kwargs: Any) -> ContextManager[Any]:
        return _NullContextManager()

    def count_exceptions(self, *args: Any, **kwargs: Any) -> ContextManager[Any]:
        return _NullContextManager()


class _NullRegistry:
    """Registro vacío compatible con la API utilizada en el proyecto."""

    _names_to_collectors: dict[str, Any]

    def __init__(self) -> None:
        self._names_to_collectors = {}

    def register(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - no-op
        return None

    def unregister(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - no-op
        return None


_prometheus_client: Any | None = None
with suppress(Exception):
    _prometheus_client = importlib.import_module("prometheus_client")

_metrics_module: Any | None = None
if _prometheus_client is not None:
    with suppress(Exception):
        _metrics_module = importlib.import_module("prometheus_client.metrics")

_Counter: Callable[..., Metric] | None = (
    getattr(_prometheus_client, "Counter", None) if _prometheus_client else None
)
_Gauge: Callable[..., Metric] | None = (
    getattr(_prometheus_client, "Gauge", None) if _prometheus_client else None
)
_Histogram: Callable[..., Metric] | None = (
    getattr(_prometheus_client, "Histogram", None) if _prometheus_client else None
)
_start_wsgi_server: Callable[..., Any] | None = (
    getattr(_prometheus_client, "start_wsgi_server", None) if _prometheus_client else None
)
_REGISTRY: Any | None = (
    getattr(_prometheus_client, "REGISTRY", None) if _prometheus_client else None
)

HAVE_PROM = _prometheus_client is not None and all(
    metric is not None for metric in (_Counter, _Gauge, _Histogram)
)

if _metrics_module is not None:
    MetricWrapperBase = getattr(_metrics_module, "MetricWrapperBase", _NullMetric)
else:
    class MetricWrapperBase(_NullMetric):  # type: ignore[misc]
        """Fallback minimal para anotaciones de tipo."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__()

REGISTRY = _REGISTRY if _REGISTRY is not None else _NullRegistry()


def _instantiate(metric_factory: Callable[..., Metric] | None, *args: Any, **kwargs: Any) -> Metric:
    if metric_factory is None:
        return _NullMetric()
    try:
        return metric_factory(*args, **kwargs)
    except Exception:  # pragma: no cover - error defensivo
        return _NullMetric()


def Counter(*args: Any, **kwargs: Any) -> Metric:
    """Devuelve un ``Counter`` real o un sustituto nulo."""

    return _instantiate(_Counter, *args, **kwargs)


def Gauge(*args: Any, **kwargs: Any) -> Metric:
    """Devuelve un ``Gauge`` real o un sustituto nulo."""

    return _instantiate(_Gauge, *args, **kwargs)


def Histogram(*args: Any, **kwargs: Any) -> Metric:
    """Devuelve un ``Histogram`` real o un sustituto nulo."""

    return _instantiate(_Histogram, *args, **kwargs)


def start_wsgi_server(*args: Any, **kwargs: Any) -> tuple[Any, Any] | Any | None:
    """Inicia el servidor WSGI cuando Prometheus está disponible."""

    if _start_wsgi_server is None:
        return None, None
    try:
        return _start_wsgi_server(*args, **kwargs)
    except Exception:  # pragma: no cover - error defensivo
        return None, None
