"""
Tests for the :class:`core.supervisor.Supervisor` class.

These tests focus on the basic heartbeat functionality and the ability to
record heartbeats for tasks.  We avoid testing restart logic or
watchdog timers, which depend on asynchronous timing and external
services.
"""

import asyncio
import sys
import types
import unittest

# Stub external modules used by the supervisor's dependencies
sys.modules.setdefault('dotenv', types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))
sys.modules.setdefault('colorlog', types.SimpleNamespace())
sys.modules.setdefault('colorama', types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda *args, **kwargs: None))
sys.modules.setdefault('ccxt', types.SimpleNamespace(binance=lambda *args, **kwargs: types.SimpleNamespace(set_sandbox_mode=lambda *a, **kw: None)))
sys.modules.setdefault('filelock', types.SimpleNamespace(FileLock=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)))
sys.modules.setdefault('prometheus_client', types.SimpleNamespace())
sys.modules.setdefault('psutil', types.SimpleNamespace())

from core.supervisor import Supervisor


class SupervisorTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_records_timestamp(self):
        """Calling beat() should record the heartbeat time and reset backoff."""
        sup = Supervisor()
        sup.beat('worker')
        # After a beat call, there should be an entry in task_heartbeat
        self.assertIn('worker', sup.task_heartbeat)
        self.assertEqual(sup.last_function, 'worker')
        # Backoff should be reset to 0
        self.assertEqual(sup.task_backoff['worker'], 0)

    async def test_tick_alias(self):
        """tick() should call beat() for backward compatibility."""
        sup = Supervisor()
        sup.tick('fetcher')
        self.assertIn('fetcher', sup.task_heartbeat)

    async def test_shutdown_completes(self):
        """shutdown() should set the stop event and cancel tasks without errors."""
        async def dummy_task():
            await asyncio.sleep(0.01)

        sup = Supervisor()
        # Register a supervised task
        # We simulate by creating a custom coroutine and scheduling it
        coro = dummy_task()
        task_name = 'dummy'
        sup.tasks[task_name] = asyncio.create_task(coro)
        # Now shutdown and ensure no exceptions are raised
        try:
            await sup.shutdown()
        except Exception as exc:
            self.fail(f"Supervisor.shutdown raised an exception: {exc}")
        # After shutdown the stop_event should be set
        self.assertTrue(sup.stop_event.is_set())


if __name__ == '__main__':
    unittest.main()