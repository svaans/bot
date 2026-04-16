# core/vela/metrics_definitions.py — histogramas/counters del pipeline de velas
from __future__ import annotations

import os
from typing import Any

from core.utils.metrics_compat import Counter, Histogram

try:  # pragma: no cover - métricas opcionales
    from core.metrics import (
        BUFFER_SIZE_V2,
        CANDLES_PROCESSED_TOTAL,
        ENTRADAS_RECHAZADAS_V2,
        EVAL_INTENTOS_TOTAL,
        LAST_BAR_AGE,
        WARMUP_RESTANTE,
    )
except Exception:  # pragma: no cover - fallback si core.metrics no está disponible
    class _NullMetric:
        def labels(self, *_args: Any, **_kwargs: Any) -> "_NullMetric":  # type: ignore[name-defined]
            return self

        def set(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    WARMUP_RESTANTE = _NullMetric()  # type: ignore[assignment]
    LAST_BAR_AGE = _NullMetric()  # type: ignore[assignment]
    BUFFER_SIZE_V2 = _NullMetric()  # type: ignore[assignment]
    ENTRADAS_RECHAZADAS_V2 = Counter(
        "procesar_vela_entradas_rechazadas_total_v2",
        "Entradas rechazadas tras validaciones finales",
        ["symbol", "timeframe", "reason"],
    )
    CANDLES_PROCESSED_TOTAL = Counter(
        "candles_processed_total",
        "Velas cerradas procesadas por símbolo y timeframe",
        ["symbol", "timeframe"],
    )
    EVAL_INTENTOS_TOTAL = Counter(
        "eval_intentos_total",
        "Intentos de evaluación clasificados por etapa",
        ["symbol", "timeframe", "etapa"],
    )

try:  # Preferir constante compartida para coherencia con warmup
    from core.data.bootstrap import MIN_BARS as _DEFAULT_MIN_BARS
except Exception:  # pragma: no cover - fallback cuando no existe el módulo
    _DEFAULT_MIN_BARS = int(os.getenv("MIN_BARS", "400"))

DEFAULT_MIN_BARS = _DEFAULT_MIN_BARS

_DEFAULT_LATENCY_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5)

EVAL_LATENCY = Histogram(
    "procesar_vela_eval_latency_seconds",
    "Latencia total de procesar_vela (parseo + buffers + estrategia)",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
PARSE_LATENCY = Histogram(
    "procesar_vela_parse_latency_seconds",
    "Latencia de normalización y sanitización de velas",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
GATING_LATENCY = Histogram(
    "procesar_vela_gating_latency_seconds",
    "Latencia de buffers y validaciones previas a la estrategia",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
STRATEGY_LATENCY = Histogram(
    "procesar_vela_strategy_latency_seconds",
    "Latencia de evaluación de estrategias y scoring",
    buckets=_DEFAULT_LATENCY_BUCKETS,
)
HANDLER_EXCEPTIONS = Counter(
    "procesar_vela_exceptions_total",
    "Excepciones no controladas en procesar_vela",
)
CANDLES_IGNORADAS = Counter(
    "procesar_vela_ignorada_total",
    "Velas ignoradas por validación previa o falta de datos",
    ["reason"],
)
ENTRADAS_CANDIDATAS = Counter(
    "procesar_vela_entradas_candidatas_total",
    "Entradas candidatas generadas por símbolo",
    ["symbol", "side"],
)
ENTRADAS_ABIERTAS = Counter(
    "procesar_vela_entradas_abiertas_total",
    "Entradas abiertas (OrderManager)",
    ["symbol", "side"],
)
SPREAD_GUARD_MISSING = Counter(
    "procesar_vela_spread_guard_missing_total",
    "Velas sin spread_ratio cuando la guardia de spread está activa",
    ["symbol", "timeframe"],
)
SPREAD_GUARD_ERRORS = Counter(
    "procesar_vela_spread_guard_errors_total",
    "Excepciones al evaluar spread_guard (el pipeline sigue; revisar guardia)",
    ["symbol"],
)
