from __future__ import annotations

from collections import deque
from typing import Any, Callable

import pytest


@pytest.fixture(autouse=True)
def stub_data_feed(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Reemplaza ``DataFeed`` por un stub liviano durante los tests."""

    instances: list[Any] = []

    class StubFeed:
        def __init__(self, intervalo: str, **kwargs: Any) -> None:
            self.intervalo = intervalo
            self.kwargs = kwargs
            self.handler_timeout = kwargs.get("handler_timeout")
            self.inactivity_intervals = kwargs.get("inactivity_intervals")
            self.queue_max = kwargs.get("queue_max")
            self.queue_policy = kwargs.get("queue_policy")
            self.monitor_interval = kwargs.get("monitor_interval")
            self.backpressure = kwargs.get("backpressure")
            self.cancel_timeout = kwargs.get("cancel_timeout")
            self._managed_by_trader = kwargs.get("_managed_by_trader", False)
            self.detener_calls = 0
            self.precargar_calls: list[dict[str, Any]] = []
            self.escuchar_calls: deque[dict[str, Any]] = deque()
            instances.append(self)

        async def detener(self) -> None:
            self.detener_calls += 1

        async def escuchar(
            self,
            symbols: list[str] | tuple[str, ...],
            handler: Callable[[dict], Any],
            *,
            cliente: Any | None = None,
        ) -> None:
            self.escuchar_calls.append(
                {
                    "symbols": tuple(symbols),
                    "handler": handler,
                    "cliente": cliente,
                }
            )

        async def precargar(
            self,
            symbols: list[str] | tuple[str, ...],
            *,
            cliente: Any | None = None,
            minimo: int | None = None,
        ) -> None:
            self.precargar_calls.append(
                {
                    "symbols": tuple(symbols),
                    "cliente": cliente,
                    "minimo": minimo,
                }
            )

    monkeypatch.setattr("core.trader_modular.DataFeed", StubFeed)
    return instances