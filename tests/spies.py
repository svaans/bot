from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Tuple

import functools


@dataclass
class CallCounter:
    """Cuenta invocaciones y guarda los últimos argumentos recibidos."""

    called: bool = False
    calls: int = 0
    last_args: Tuple[Any, ...] = field(default_factory=tuple)
    last_kwargs: dict[str, Any] = field(default_factory=dict)

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.called = True
        self.calls += 1
        self.last_args = args
        self.last_kwargs = dict(kwargs)


def spy_trader(monkeypatch: Any, trader: Any) -> CallCounter:
    """Inyecta un espía sobre ``Trader._procesar_vela`` preservando la lógica."""

    counter = CallCounter()
    original = trader._procesar_vela

    @functools.wraps(original)
    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        counter(*args, **kwargs)
        return await original(*args, **kwargs)

    monkeypatch.setattr(trader, "_procesar_vela", _wrapper)
    return counter
