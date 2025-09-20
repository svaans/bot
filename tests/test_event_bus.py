"""
Tests for the :class:`core.event_bus.EventBus` class.

These tests verify that the event bus can subscribe callbacks, publish
events and dispatch them asynchronously.  We stub out external
dependencies as in other tests to isolate the behaviour.
"""

import asyncio
import sys
import types
import unittest

# Install stubs for external modules (reuse the same approach as other tests)
sys.modules.setdefault('dotenv', types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))
sys.modules.setdefault('colorlog', types.SimpleNamespace())
sys.modules.setdefault('colorama', types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda *args, **kwargs: None))
sys.modules.setdefault('ccxt', types.SimpleNamespace(binance=lambda *args, **kwargs: types.SimpleNamespace(set_sandbox_mode=lambda *a, **kw: None)))
sys.modules.setdefault('filelock', types.SimpleNamespace(FileLock=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)))
sys.modules.setdefault('prometheus_client', types.SimpleNamespace())
sys.modules.setdefault('psutil', types.SimpleNamespace())

from core.event_bus import EventBus


class EventBusTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_and_publish(self):
        """Callbacks subscribed to an event_type should receive published data."""
        bus = EventBus()
        received: list[int] = []

        async def callback(data):
            # record the received event
            received.append(data)

        # subscribe to event
        bus.subscribe('tick', callback)
        # publish event
        await bus.publish('tick', 42)
        # Give the dispatcher task time to run
        await asyncio.sleep(0.01)
        self.assertEqual(received, [42])

    async def test_multiple_listeners(self):
        """Multiple listeners on the same event_type should all be invoked."""
        bus = EventBus()
        rec1: list[int] = []
        rec2: list[int] = []

        async def cb1(data):
            rec1.append(data)

        async def cb2(data):
            rec2.append(data)

        bus.subscribe('ping', cb1)
        bus.subscribe('ping', cb2)
        await bus.publish('ping', 7)
        await asyncio.sleep(0.01)
        self.assertEqual(rec1, [7])
        self.assertEqual(rec2, [7])

    async def test_close_stops_dispatcher(self):
        """Closing the bus should cancel the dispatcher task without error."""
        bus = EventBus()
        async def dummy(_):
            pass
        bus.subscribe('foo', dummy)
        await bus.publish('foo', 1)
        await asyncio.sleep(0.01)
        # Now close the bus and ensure no exceptions are raised
        await bus.close()
        # After close, publishing should not raise but the event won't be delivered
        # because _closed flag is set; we can't assert internal behaviour but can ensure no error
        try:
            await bus.publish('foo', 2)
        except Exception as exc:
            self.fail(f"EventBus.publish raised after close: {exc}")


if __name__ == '__main__':
    unittest.main()