# core/vela/circuit_breaker.py — circuit breaker de creación de órdenes
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

_ORDER_CIRCUIT_MAX_FAILURES = 3
_ORDER_CIRCUIT_OPEN_SECONDS = 30.0
_ORDER_CIRCUIT_RESET_AFTER = 120.0


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

    def record_failure(self, now: float) -> bool:
        """Registra una falla y devuelve si el circuito debe abrirse."""

        if now - self.last_failure >= _ORDER_CIRCUIT_RESET_AFTER:
            self.failures = 0
        self.last_failure = now
        self.failures += 1
        if self.failures >= _ORDER_CIRCUIT_MAX_FAILURES:
            self.opened_until = now + _ORDER_CIRCUIT_OPEN_SECONDS
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


_DEFAULT_ORDER_CIRCUIT_STORE = OrderCircuitBreakerStore()


def _resolve_order_circuit_store(trader: Any) -> OrderCircuitBreakerStore:
    store = getattr(trader, "order_circuit_store", None)
    if isinstance(store, OrderCircuitBreakerStore):
        return store
    return _DEFAULT_ORDER_CIRCUIT_STORE
