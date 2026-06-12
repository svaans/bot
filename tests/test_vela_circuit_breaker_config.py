"""Fase 15 — Regresión: circuit breaker respeta parámetros de Config.

Antes del fix, _CircuitBreakerState.record_failure usaba exclusivamente constantes
de módulo (_ORDER_CIRCUIT_MAX_FAILURES etc.), ignorando cualquier valor en Config.
_resolve_circuit_params existía pero nunca era llamada.

El fix:
  1. record_failure acepta max_failures / open_seconds / reset_after opcionales.
  2. _abrir_orden acepta circuit_params: tuple[int, float, float] | None.
  3. pipeline_procesar pasa _resolve_circuit_params(trader) a _abrir_orden.
"""
from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any

import pytest

from core.vela.circuit_breaker import (
    OrderCircuitBreakerStore,
    _CircuitBreakerState,
    _resolve_circuit_params,
)
from core.orders.order_open_status import OrderOpenStatus


# ---------------------------------------------------------------------------
# _CircuitBreakerState.record_failure acepta params desde Config
# ---------------------------------------------------------------------------

def test_record_failure_opens_at_custom_max_failures() -> None:
    """Con max_failures=1 el CB se abre en el primer fallo."""
    state = _CircuitBreakerState()
    now = time.monotonic()
    opened = state.record_failure(now, max_failures=1, open_seconds=10.0, reset_after=60.0)
    assert opened is True
    assert state.opened_until > now


def test_record_failure_does_not_open_before_custom_max() -> None:
    """Con max_failures=5 el CB no se abre en el tercer fallo."""
    state = _CircuitBreakerState()
    now = time.monotonic()
    for _ in range(3):
        opened = state.record_failure(now, max_failures=5, open_seconds=10.0, reset_after=60.0)
    assert opened is False


def test_record_failure_uses_custom_open_seconds() -> None:
    """open_seconds controla durante cuánto tiempo permanece abierto el CB."""
    state = _CircuitBreakerState()
    now = time.monotonic()
    state.record_failure(now, max_failures=1, open_seconds=999.0, reset_after=60.0)
    assert state.opened_until == pytest.approx(now + 999.0, abs=0.1)


def test_record_failure_resets_on_custom_reset_after() -> None:
    """Con reset_after=0 el contador se resetea en cada llamada."""
    state = _CircuitBreakerState()
    t0 = time.monotonic()
    # Primera falla
    state.record_failure(t0, max_failures=3, open_seconds=10.0, reset_after=0.0)
    assert state.failures == 1
    # Segunda falla con reset_after=0 → la diferencia de tiempo >= 0 → reset
    t1 = t0 + 0.001
    state.record_failure(t1, max_failures=3, open_seconds=10.0, reset_after=0.0)
    # Después del reset: failures vuelve a 0, luego +1 → 1
    assert state.failures == 1


def test_record_failure_falls_back_to_module_defaults_when_no_params() -> None:
    """Sin parámetros, el comportamiento es idéntico al original."""
    from core.vela.circuit_breaker import _ORDER_CIRCUIT_MAX_FAILURES
    state = _CircuitBreakerState()
    now = time.monotonic()
    opened_list = [
        state.record_failure(now) for _ in range(_ORDER_CIRCUIT_MAX_FAILURES)
    ]
    # Debe abrirse exactamente en el último intento
    assert opened_list[-1] is True
    assert all(x is False for x in opened_list[:-1])


# ---------------------------------------------------------------------------
# _resolve_circuit_params lee desde Config
# ---------------------------------------------------------------------------

def test_resolve_circuit_params_from_config() -> None:
    class _Cfg:
        order_circuit_max_failures = 7
        order_circuit_open_seconds = 45.0
        order_circuit_reset_after = 200.0

    trader = SimpleNamespace(config=_Cfg())
    max_f, open_s, reset = _resolve_circuit_params(trader)
    assert max_f == 7
    assert open_s == pytest.approx(45.0)
    assert reset == pytest.approx(200.0)


def test_resolve_circuit_params_falls_back_when_no_config() -> None:
    from core.vela.circuit_breaker import (
        _ORDER_CIRCUIT_MAX_FAILURES,
        _ORDER_CIRCUIT_OPEN_SECONDS,
        _ORDER_CIRCUIT_RESET_AFTER,
    )
    trader = SimpleNamespace()  # sin config
    max_f, open_s, reset = _resolve_circuit_params(trader)
    assert max_f == _ORDER_CIRCUIT_MAX_FAILURES
    assert open_s == pytest.approx(_ORDER_CIRCUIT_OPEN_SECONDS)
    assert reset == pytest.approx(_ORDER_CIRCUIT_RESET_AFTER)


# ---------------------------------------------------------------------------
# _abrir_orden respeta circuit_params
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_abrir_orden_opens_circuit_with_custom_max_failures() -> None:
    """Con max_failures=1 la primera excepción en crear abre el CB."""
    from core.vela.order_open import _abrir_orden

    class _FailOrders:
        def crear(self, **_kw: Any) -> None:
            raise RuntimeError("fallo simulado")

    store = OrderCircuitBreakerStore()
    with pytest.raises(Exception):
        await _abrir_orden(
            _FailOrders(),
            "BTC/USDT",
            "long",
            30_000.0,
            29_000.0,
            31_000.0,
            {},
            max_attempts=1,
            circuit_store=store,
            circuit_params=(1, 10.0, 60.0),  # max_failures=1
        )

    state = store.get_state("BTC/USDT", "long")
    # El CB debe estar abierto tras el primer fallo con max_failures=1
    assert state.opened_until > time.monotonic()


@pytest.mark.asyncio
async def test_abrir_orden_circuit_params_none_uses_defaults() -> None:
    """Sin circuit_params el comportamiento es equivalente al original."""
    from core.vela.order_open import _abrir_orden

    call_count = {"n": 0}

    class _OkOrders:
        def crear(self, **_kw: Any) -> OrderOpenStatus:
            call_count["n"] += 1
            return OrderOpenStatus.OPENED

    store = OrderCircuitBreakerStore()
    result = await _abrir_orden(
        _OkOrders(),
        "ETH/USDT",
        "long",
        2_000.0,
        1_900.0,
        2_100.0,
        {},
        circuit_store=store,
        circuit_params=None,
    )
    assert result is OrderOpenStatus.OPENED
    assert call_count["n"] == 1
