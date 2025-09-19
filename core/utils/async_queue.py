from __future__ import annotations

import asyncio
import contextlib
import functools
from collections.abc import Awaitable, Callable
from typing import Any

from .logger import configurar_logger


log = configurar_logger('async_queue', modo_silencioso=True)


class AsyncWorkerQueue:
    """Ejecuta trabajos en segundo plano usando ``asyncio``.

    Los trabajos se ejecutan fuera del camino crítico del *pipeline* para
    desacoplar operaciones de I/O (notificaciones, escrituras, etc.).
    """

    def __init__(
        self,
        name: str,
        *,
        workers: int = 1,
        maxsize: int = 0,
        exception_handler: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._name = name
        self._queue: asyncio.Queue[Callable[[], Awaitable[Any] | Any]] = (
            asyncio.Queue(maxsize)
        )
        self._workers = []
        self._workers_count = max(1, workers)
        self._closed = False
        self._exception_handler = exception_handler

    def start(self) -> None:
        """Inicializa los *workers* asociados a la cola."""

        if self._workers:
            return
        loop = asyncio.get_running_loop()
        for idx in range(self._workers_count):
            task = loop.create_task(self._worker(idx), name=f'{self._name}:{idx}')
            self._workers.append(task)

    async def _worker(self, idx: int) -> None:
        while True:
            job = await self._queue.get()
            if job is None:
                self._queue.task_done()
                break
            try:
                result = job()
                if asyncio.iscoroutine(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - logging defensivo
                if self._exception_handler:
                    try:
                        self._exception_handler(exc)
                    except Exception:  # pragma: no cover - logging
                        log.exception(
                            'Error en exception_handler de %s', self._name
                        )
                else:
                    log.exception(
                        '❌ Error ejecutando trabajo en cola %s', self._name
                    )
            finally:
                self._queue.task_done()

    def submit(
        self,
        func: Callable[..., Awaitable[Any] | Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Agrega un trabajo a la cola.

        Si la cola está llena, se descarta el trabajo más antiguo para evitar
        backpressure en el camino crítico del *pipeline*.
        """

        if self._closed:
            raise RuntimeError(f'Queue {self._name} closed')
        job = functools.partial(func, *args, **kwargs)
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            try:
                _ = self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                pass
            finally:
                self._queue.put_nowait(job)
                log.warning('⚠️ Cola %s saturada; descartando trabajo antiguo', self._name)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in self._workers:
            await self._queue.put(None)  # type: ignore[arg-type]
        await self._queue.join()
        for task in self._workers:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        self._workers.clear()


class AsyncBatchQueue(AsyncWorkerQueue):
    """Procesa elementos en lotes para reducir operaciones de escritura."""

    def __init__(
        self,
        name: str,
        handler: Callable[[list[Any]], Awaitable[Any] | Any],
        *,
        batch_size: int = 32,
        flush_interval: float = 0.5,
        maxsize: int = 0,
    ) -> None:
        super().__init__(name, workers=1, maxsize=maxsize)
        self._handler = handler
        self._batch_size = max(1, batch_size)
        self._flush_interval = max(0.05, flush_interval)
        self._buffer: list[Any] = []

    async def _worker(self, idx: int) -> None:
        while True:
            try:
                job = await asyncio.wait_for(
                    self._queue.get(), timeout=self._flush_interval
                )
            except asyncio.TimeoutError:
                job = None
            if job is None:
                if self._buffer:
                    await self._flush()
                if self._closed:
                    self._queue.task_done()
                    break
                continue
            try:
                result = job()
                if asyncio.iscoroutine(result):
                    result = await result
                self._buffer.append(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - logging defensivo
                log.exception('❌ Error en productor de %s: %s', self._name, exc)
            finally:
                self._queue.task_done()
            if len(self._buffer) >= self._batch_size:
                await self._flush()

    async def _flush(self) -> None:
        if not self._buffer:
            return
        payload = self._buffer
        self._buffer = []
        try:
            result = self._handler(payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # pragma: no cover - logging defensivo
            log.exception('❌ Error procesando batch en cola %s', self._name)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._queue.put(None)  # type: ignore[arg-type]
        await self._queue.join()
        await self._flush()
        for task in self._workers:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        self._workers.clear()