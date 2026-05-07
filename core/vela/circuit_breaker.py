# core/vela/circuit_breaker.py — circuit breaker de creación de órdenes
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

# Valores por defecto; anulables vía Config (campos ``order_circuit_*``) o
# variables de entorno como fallback para entornos sin Config completo.
_ORDER_CIRCUIT_MAX_FAILURES: int = int(
    os.getenv("ORDER_CIRCUIT_MAX_FAILURES", "3")
)
_ORDER_CIRCUIT_OPEN_SECONDS: float = float(
    os.getenv("ORDER_CIRCUIT_OPEN_SECONDS", "30.0")
)
_ORDER_CIRCUIT_RESET_AFTER: float = float(
    os.getenv("ORDER_CIRCUIT_RESET_AFTER", "120.0")
)


@dataclass
class _CircuitBreakerState:
    """Estado interno del circuit breaker por símbolo/dirección."""

    failures: int = 0
    opened_until: float = 0.0
    last_failure: float = 0.0

    def reset_if_idle(self, now: float, reset_after: float) -> None:
        """Reinicia contadores si no hubo fallas recientes."""

        if self.failures and now - self.last_failure >= reset_after:
            self.failures = 0
            self.opened_until = 0.0

    def record_success(self) -> None:
        """Resetea el estado tras un intento exitoso."""

        self.failures = 0
        self.opened_until = 0.0

    def record_failure(
        self,
        now: float,
        *,
        max_failures: int | None = None,
        open_seconds: float | None = None,
        reset_after: float | None = None,
    ) -> bool:
        """Registra una falla y devuelve si el circuito debe abrirse.

        Los parámetros opcionales permiten que el caller inyecte valores
        desde Config (via ``_resolve_circuit_params``); si se omiten se usan
        los defaults de módulo (env-overridables: ORDER_CIRCUIT_*).
        """

        mf = max_failures if max_failures is not None else _ORDER_CIRCUIT_MAX_FAILURES
        os_ = open_seconds if open_seconds is not None else _ORDER_CIRCUIT_OPEN_SECONDS
        ra = reset_after if reset_after is not None else _ORDER_CIRCUIT_RESET_AFTER

        if now - self.last_failure >= ra:
            self.failures = 0
        self.last_failure = now
        self.failures += 1
        if self.failures >= mf:
            self.opened_until = now + os_
            return True
        return False


class OrderCircuitBreakerOpen(RuntimeError):
    """Excepción específica cuando el circuit breaker impide abrir órdenes."""

    def __init__(self, retry_after: float, state: _CircuitBreakerState) -> None:
        super().__init__("order circuit breaker open")
        self.retry_after = retry_after
        self.state = state


class OrderCircuitBreakerStore:
    """Circuit breakers por sesión (una instancia por ``Trader`` en producción)."""

    __slots__ = ("_circuits",)

    def __init__(self) -> None:
        self._circuits: Dict[str, _CircuitBreakerState] = {}

    def get_state(self, symbol: str, side: str) -> _CircuitBreakerState:
        key = f"{symbol.upper()}:{side.lower()}"
        state = self._circuits.get(key)
        if state is None:
            state = _CircuitBreakerState()
            self._circuits[key] = state
        return state

    def clear(self) -> None:
        self._circuits.clear()


# Singleton de último recurso; sólo se usa cuando el Trader no tiene su propio
# ``order_circuit_store``. En tests, cada Trader debería proveer el suyo propio
# para evitar polución de estado entre tests.
_DEFAULT_ORDER_CIRCUIT_STORE = OrderCircuitBreakerStore()


def _resolve_order_circuit_store(trader: Any) -> OrderCircuitBreakerStore:
    store = getattr(trader, "order_circuit_store", None)
    if isinstance(store, OrderCircuitBreakerStore):
        return store
    return _DEFAULT_ORDER_CIRCUIT_STORE


def _resolve_circuit_params(trader: Any) -> tuple[int, float, float]:
    """Devuelve ``(max_failures, open_seconds, reset_after)`` desde Config o módulo."""
    cfg = getattr(trader, "config", None)
    max_f = int(getattr(cfg, "order_circuit_max_failures", _ORDER_CIRCUIT_MAX_FAILURES))
    open_s = float(getattr(cfg, "order_circuit_open_seconds", _ORDER_CIRCUIT_OPEN_SECONDS))
    reset = float(getattr(cfg, "order_circuit_reset_after", _ORDER_CIRCUIT_RESET_AFTER))
    return max_f, open_s, reset
