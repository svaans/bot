import os
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

import grpc

from core import orders_pb2, orders_pb2_grpc

log = logging.getLogger(__name__)

WORKER_HOST = os.getenv("ORDERS_WORKER_HOST", "localhost")
WORKER_PORT = int(os.getenv("ORDERS_WORKER_PORT", "9100"))
_ADDRESS = f"{WORKER_HOST}:{WORKER_PORT}"

_channel = grpc.insecure_channel(_ADDRESS)
_stub = orders_pb2_grpc.OrderWriterStub(_channel)

_executor = ThreadPoolExecutor(max_workers=2)
_pending = set()


def _send_batch(batch: orders_pb2.OrdersBatch) -> None:
    try:
        _stub.WriteOrders(batch, timeout=10)
    except Exception as e:  # noqa: BLE001
        log.error("Error enviando operaciones al worker: %s", e)
        raise


def send_async(batch: orders_pb2.OrdersBatch) -> None:
    fut = _executor.submit(_send_batch, batch)
    _pending.add(fut)
    fut.add_done_callback(_pending.discard)


def wait_pending() -> None:
    for fut in list(_pending):
        try:
            fut.result()
        except Exception:
            pass