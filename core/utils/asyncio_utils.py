from __future__ import annotations
import asyncio
from typing import Awaitable, TypeVar

T = TypeVar("T")

async def run_with_timeout(coro: Awaitable[T], timeout: float) -> T:
    """Ejecuta ``coro`` con un timeout explícito."""
    return await asyncio.wait_for(coro, timeout)