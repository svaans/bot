# tests/test_main_utils.py
import asyncio
import types
import pytest
import importlib

mod = importlib.import_module('main')

@pytest.mark.asyncio
async def test_maybe_await_variants():
    assert await mod._maybe_await(42) == 42
    async def coro(): return "ok"
    assert await mod._maybe_await(coro()) == "ok"

def test_safe_call_happy_path(capsys):
    class Obj: 
        def ping(self): print("pong")
    mod._safe_call(Obj(), "ping")
    assert "pong" in capsys.readouterr().out

def test_safe_call_missing_method():
    class Obj: pass
    assert mod._safe_call(Obj(), "nope") is None

@pytest.mark.asyncio
async def test_safe_acall_async_timeout(capsys):
    class Obj:
        async def slow(self): await asyncio.sleep(0.05)
    await mod._safe_acall(Obj(), "slow", timeout=0.01)
    assert "Timeout en" in capsys.readouterr().out
