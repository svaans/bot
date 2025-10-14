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

    def start_supervision(self) -> None:
        self.started = True
        return None

    def supervised_task(
        self,
        factory: Callable[[], Any],
        *,
        name: str | None = None,
        expected_interval: int | None = None,
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

    def beat(self, name: str, cause: str | None = None) -> None:  # pragma: no cover - dummy interface
        self.beats.append((name, cause))

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
        self.df_queue_policy = "drop_oldest"
        self.monitor_interval = 5.0
        self.df_backpressure = True
        self.orders_retry_persistencia_enabled = False
        self.trader_purge_historial_enabled = False
        self.metrics_extended_enabled = False
        self.datafeed_debug_wrapper_enabled = False
        self.orders_flush_periodico_enabled = False
        self.orders_limit_enabled = False
        self.funding_enabled = False
        self.backfill_ventana_enabled = False
        for key, value in overrides.items():
            setattr(self, key, value)