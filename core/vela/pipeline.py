# core/vela/pipeline.py — API estable del pipeline (reexporta subsistemas y ``procesar_vela``)
from __future__ import annotations

from core.vela.buffers import (
    BufferManager,
    _buffers,
    _resolve_buffer_manager,
    create_buffer_manager,
    get_buffer_manager,
)
from core.vela.circuit_breaker import (
    OrderCircuitBreakerOpen,
    OrderCircuitBreakerStore,
    _DEFAULT_ORDER_CIRCUIT_STORE,
    _ORDER_CIRCUIT_MAX_FAILURES,
    _resolve_order_circuit_store,
)
from core.vela.helpers import _resolve_entrada_cooldown_tras_crear_failed_sec
from core.vela.metrics_definitions import (
    BUFFER_SIZE_V2,
    CANDLES_IGNORADAS,
    CANDLES_PROCESSED_TOTAL,
    DEFAULT_MIN_BARS,
    ENTRADAS_ABIERTAS,
    ENTRADAS_CANDIDATAS,
    ENTRADAS_RECHAZADAS_V2,
    EVAL_INTENTOS_TOTAL,
    EVAL_LATENCY,
    GATING_LATENCY,
    HANDLER_EXCEPTIONS,
    LAST_BAR_AGE,
    PARSE_LATENCY,
    SPREAD_GUARD_ERRORS,
    SPREAD_GUARD_MISSING,
    STRATEGY_LATENCY,
    WARMUP_RESTANTE,
)
from core.vela.order_open import _ORDER_CREATION_MAX_ATTEMPTS, _abrir_orden
from core.vela.pipeline_procesar import procesar_vela
from core.vela.spread import spread_gate_default

__all__ = [
    "DEFAULT_MIN_BARS",
    "BUFFER_SIZE_V2",
    "BufferManager",
    "CANDLES_IGNORADAS",
    "CANDLES_PROCESSED_TOTAL",
    "ENTRADAS_ABIERTAS",
    "ENTRADAS_CANDIDATAS",
    "ENTRADAS_RECHAZADAS_V2",
    "EVAL_INTENTOS_TOTAL",
    "EVAL_LATENCY",
    "GATING_LATENCY",
    "HANDLER_EXCEPTIONS",
    "LAST_BAR_AGE",
    "OrderCircuitBreakerOpen",
    "OrderCircuitBreakerStore",
    "PARSE_LATENCY",
    "SPREAD_GUARD_ERRORS",
    "SPREAD_GUARD_MISSING",
    "STRATEGY_LATENCY",
    "WARMUP_RESTANTE",
    "_DEFAULT_ORDER_CIRCUIT_STORE",
    "_ORDER_CIRCUIT_MAX_FAILURES",
    "_ORDER_CREATION_MAX_ATTEMPTS",
    "_abrir_orden",
    "_buffers",
    "_resolve_buffer_manager",
    "_resolve_entrada_cooldown_tras_crear_failed_sec",
    "_resolve_order_circuit_store",
    "create_buffer_manager",
    "get_buffer_manager",
    "procesar_vela",
    "spread_gate_default",
]
