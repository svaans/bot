# core/orders/real_orders_async.py — utilidades asyncio para :mod:`core.orders.real_orders`
from __future__ import annotations

import asyncio
import inspect
from typing import Any, Awaitable, Coroutine, TypeVar

T = TypeVar("T")


def as_coroutine(awaitable: Awaitable[T]) -> Coroutine[Any, Any, T]:
    if asyncio.iscoroutine(awaitable):
        return awaitable  # type: ignore[return-value]

    async def _wrapper() -> T:
        return await awaitable

    return _wrapper()


def run_coroutine_sync(awaitable: Awaitable[T]) -> T:
    coro = as_coroutine(awaitable)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    if loop.is_running():
        new_loop = asyncio.new_event_loop()
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
    return loop.run_until_complete(coro)


def resolve_maybe_awaitable(value: Any) -> Any:
    if inspect.isawaitable(value):
        return run_coroutine_sync(value)
    return value
