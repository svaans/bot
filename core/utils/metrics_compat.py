"""Compatibilidad ligera para métricas Prometheus opcionales."""
from __future__ import annotations

import time

from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, Iterable, Tuple, Callable, TypeVar, cast

try:  # pragma: no cover - dependencias opcionales
    from prometheus_client import Counter as PromCounter
    from prometheus_client import Gauge as PromGauge
    from prometheus_client import Histogram as PromHistogram
    from prometheus_client import start_wsgi_server as prom_start_wsgi_server
    from prometheus_client import REGISTRY as PROM_REGISTRY
except Exception:  # pragma: no cover - fallback
    PromCounter = PromGauge = PromHistogram = None  # type: ignore[assignment]

    prom_start_wsgi_server = None  # type: ignore[assignment]
    PROM_REGISTRY = None  # type: ignore[assignment]

HAVE_PROM = PromCounter is not None

__all__ = ["Counter", "Gauge", "Histogram", "HAVE_PROM", "start_wsgi_server"]


@dataclass
class _BaseMetric:
    name: str
    documentation: str
    labelnames: Tuple[str, ...] = ()
    _children: Dict[Tuple[str, ...], "_BaseMetric"] = field(default_factory=dict)
    _metric: Any | None = None

    def labels(self, *values: str, **kwargs: str) -> "_BaseMetric":
        if not self.labelnames:
            return self
        if kwargs:
            if values:
                raise TypeError("labels() no acepta argumentos mixtos posicionales y nombrados")
            missing = [name for name in self.labelnames if name not in kwargs]
            if missing:
                raise ValueError(f"Faltan labels requeridas: {', '.join(missing)}")
            extra = sorted(set(kwargs) - set(self.labelnames))
            if extra:
                raise ValueError(f"Labels desconocidas: {', '.join(extra)}")
            ordered_values = tuple(kwargs[name] for name in self.labelnames)
        else:
            if len(values) != len(self.labelnames):
                raise ValueError("Número de labels inválido")
            ordered_values = tuple(values)

        key = ordered_values
        if key not in self._children:
            child = object.__new__(self.__class__)
            child.name = self.name
            child.documentation = self.documentation
            child.labelnames = ()
            child._children = {}
            if hasattr(self, "_value"):
                child._value = 0.0
            if hasattr(self, "_observations"):
                child._observations = []
            child._metric = self._metric.labels(*ordered_values) if self._metric is not None else None
            self._children[key] = child
        return self._children[key]


class Counter(_BaseMetric):
    def __init__(self, name: str, documentation: str, labelnames: Iterable[str] | None = None) -> None:
        super().__init__(name, documentation, tuple(labelnames or ()))
        self._value: float = 0.0
        self._metric = _reuse_or_create_metric(PromCounter, name, documentation, self.labelnames)
    def inc(self, amount: float = 1.0) -> None:
        self._value += amount
        if self._metric is not None:
            self._metric.inc(amount)


class Gauge(_BaseMetric):
    def __init__(self, name: str, documentation: str, labelnames: Iterable[str] | None = None) -> None:
        super().__init__(name, documentation, tuple(labelnames or ()))
        self._value: float = 0.0
        self._metric = _reuse_or_create_metric(PromGauge, name, documentation, self.labelnames)

    def inc(self, amount: float = 1.0) -> None:
        self._value += amount
        if self._metric is not None:
            self._metric.inc(amount)

    def dec(self, amount: float = 1.0) -> None:
        self._value -= amount
        if self._metric is not None:
            self._metric.dec(amount)

    def set(self, value: float) -> None:
        self._value = value
        if self._metric is not None:
            self._metric.set(value)

_F = TypeVar("_F", bound=Callable[..., Any])


class _HistogramTimer:
    """Pequeño temporizador compatible con ``Histogram.time()`` de Prometheus."""

    def __init__(self, histogram: "Histogram") -> None:
        self._histogram = histogram
        self._start: float | None = time.perf_counter()

    def _restart(self) -> None:
        self._start = time.perf_counter()

    def _duration(self) -> float:
        if self._start is None:
            return 0.0
        return max(time.perf_counter() - self._start, 0.0)

    def __enter__(self) -> "_HistogramTimer":
        self._restart()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover - trivial
        self.observe_duration()

    def observe_duration(self) -> float:
        """Registra la duración transcurrida desde la creación del temporizador."""

        duration = self._duration()
        self._histogram.observe(duration)
        self._start = None
        return duration

    def labels(self, *values: str, **kwargs: str) -> "_HistogramTimer":
        hist = self._histogram.labels(*values, **kwargs)
        return hist.time()

    def __call__(self, func: _F) -> _F:
        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any):
            with self._histogram.time():
                return func(*args, **kwargs)

        return cast(_F, wrapped)


class Histogram(_BaseMetric):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] | None = None,
        *,
        buckets: Iterable[float] | None = None,
    ) -> None:
        super().__init__(name, documentation, tuple(labelnames or ()))
        self._observations: list[float] = []
        kwargs = {"buckets": buckets} if buckets is not None else {}
        self._metric = _reuse_or_create_metric(PromHistogram, name, documentation, self.labelnames, **kwargs)
    def observe(self, value: float) -> None:
        self._observations.append(value)
        if self._metric is not None:
            self._metric.observe(value)

    def time(self) -> _HistogramTimer:
        return _HistogramTimer(self)


def start_wsgi_server(port: int, addr: str = "0.0.0.0") -> Any:
    """Inicia el servidor WSGI de Prometheus o un stub en memoria."""

    if prom_start_wsgi_server:
        return prom_start_wsgi_server(port, addr)

    class _DummyServer:
        def __init__(self, port: int, addr: str) -> None:
            self.port = port
            self.addr = addr

        def shutdown(self) -> None:  # pragma: no cover - no side effects
            return None

    return _DummyServer(port, addr)



def _reuse_or_create_metric(factory: Any, name: str, documentation: str, labelnames: Tuple[str, ...], **kwargs: Any) -> Any:
    """Crea la métrica Prometheus reutilizando instancias existentes si es posible."""

    if factory is None:
        return None

    try:
        return factory(name, documentation, labelnames, **kwargs)
    except ValueError as exc:  # pragma: no cover - depende del entorno de métricas
        if PROM_REGISTRY is None:
            raise exc
        existing = PROM_REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
        if existing is None:
            raise exc
        # Validar que el tipo y labels coincidan antes de reutilizar
        expected_type = getattr(factory, "_type", getattr(existing, "_type", None))
        if getattr(existing, "_type", None) != expected_type:
            raise exc
        if tuple(getattr(existing, "_labelnames", ())) != labelnames:
            raise exc
        return existing
