from __future__ import annotations

from typing import Any, Callable


class DummySupervisor:
    """Supervisor ligero para verificar interacciones del trader."""

    def __init__(self) -> None:
        self.ticks: list[tuple[str, bool]] = []
        self.started = False
        self.supervised: list[dict[str, Any]] = []
        self.beats: list[tuple[str, str | None]] = []
        self.shutdown_calls = 0
        self.expected_interval_updates: list[tuple[str, float]] = []

    def start_supervision(self) -> None:
        self.started = True
        return None

    def supervised_task(
        self,
        factory: Callable[[], Any],
        *,
        name: str | None = None,
        expected_interval: float | None = None,
        delay: float | None = None,
    ) -> None:
        self.supervised.append(
            {
                "factory": factory,
                "name": name,
                "expected_interval": expected_interval,
                "delay": delay,
            }
        )

    def beat(
        self,
        name: str,
        cause: str | None = None,
        *,
        expected_interval: float | None = None,
    ) -> None:  # pragma: no cover - dummy interface
        self.beats.append((name, cause))
        if expected_interval is not None:
            self.set_expected_interval(name, expected_interval)

    def set_expected_interval(self, name: str, interval: float | None) -> None:
        if interval is None or interval <= 0:
            return
        self.expected_interval_updates.append((name, float(interval)))

    def tick_data(self, symbol: str, reinicio: bool = False) -> None:
        self.ticks.append((symbol, reinicio))

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class DummyConfig:
    """Config básica con overrides dinámicos para los tests."""

    def __init__(self, **overrides: Any) -> None:
        self.symbols = ["BTCUSDT"]
        self.intervalo_velas = "1m"
        self.modo_real = False
        self.max_spread_ratio = 0.0
        self.spread_dynamic = False
        self.spread_guard_window = 50
        self.spread_guard_hysteresis = 0.15
        self.spread_guard_max_limit = 0.05
        self.handler_timeout = 2.0
        self.inactivity_intervals = 10
        self.df_queue_default_limit = 2000
        self.df_queue_min_recommended = 16
        self.df_queue_policy = "drop_oldest"
        self.monitor_interval = 5.0
        self.df_backpressure = True
        self.orders_retry_persistencia_enabled = False
        self.trader_purge_historial_enabled = False
        self.metrics_extended_enabled = False
        self.datafeed_debug_wrapper_enabled = False
        self.indicadores_incremental_enabled = False
        self.orders_flush_periodico_enabled = False
        self.orders_limit_enabled = False
        self.orders_execution_policy = "market"
        self.orders_execution_policy_by_symbol: dict[str, str] = {}
        self.orders_reconcile_enabled = False
        self.funding_enabled = False
        self.funding_warning_threshold = 0.0005
        self.funding_score_penalty_enabled = False
        self.funding_score_penalty = 0.0
        self.funding_score_bonus = 0.0
        self.funding_cache_ttl = 300
        self.funding_symbol_overrides: dict[str, str] = {}
        self.backfill_ventana_enabled = False
        self.backfill_ventana_window: int | None = None
        self.backfill_max_candles = 1000
        for key, value in overrides.items():
            setattr(self, key, value)
