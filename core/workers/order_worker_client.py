import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

import requests

log = logging.getLogger(__name__)

WORKER_URL = os.getenv("ORDER_WORKER_URL", "http://localhost:9100/orders")

_executor = ThreadPoolExecutor(max_workers=2)
_pending = set()


def _post_orders(ops: List[Dict]) -> None:
    try:
        res = requests.post(WORKER_URL, json=ops, timeout=10)
        res.raise_for_status()
    except Exception as e:  # noqa: BLE001
        log.error("Error enviando operaciones al worker: %s", e)
        raise


def send_async(ops: List[Dict]) -> None:
    fut = _executor.submit(_post_orders, ops)
    _pending.add(fut)
    fut.add_done_callback(_pending.discard)


def wait_pending() -> None:
    for fut in list(_pending):
        try:
            fut.result()
        except Exception:
            pass