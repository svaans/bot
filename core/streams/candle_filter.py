from __future__ import annotations

import time
from collections import deque
from typing import Dict, List, Optional, Tuple


class CandleFilter:
    """Mantiene orden estricto e idempotencia de velas.

    Gestiona un watermark ``last_ts_processed`` y un conjunto LRU de
    timestamps recientes para filtrar duplicados. También ofrece un pequeño
    buffer para reordenar mensajes ligeramente fuera de orden.
    """

    def __init__(self, max_lru: int = 500, buffer_size: int = 5) -> None:
        self.max_lru = max_lru
        self.buffer_size = buffer_size
        self.last_ts_processed: Optional[int] = None
        self._recent = deque()
        self._recent_set: set[int] = set()
        self._buffer = deque()
        self.total: int = 0
        self.rejected: int = 0
        self._window_start = time.time()

    def _normalize(self, ts: int, intervalo_ms: int) -> int:
        return (ts // intervalo_ms) * intervalo_ms

    def _should_warn(self) -> bool:
        ahora = time.time()
        if ahora - self._window_start > 600:  # 10 minutos
            self._window_start = ahora
            self.total = 0
            self.rejected = 0
        return self.total > 0 and (self.rejected / self.total) > 0.05

    def push(self, candle: Dict, intervalo_ms: int) -> Tuple[List[Dict], Optional[str], bool]:
        """Añade una vela al filtro.

        Returns a tuple ``(ready, status, warn)`` where ``ready`` is a list of
        velas listas para ser procesadas en orden, ``status`` describe si la vela
        actual fue descartada (``duplicate`` o ``out_of_order`` o ``partial``) y
        ``warn`` indica si el ratio de descartes supera el 5%.
        """

        if candle.get('isFinal') is False:
            return [], 'partial', False

        ts_norm = self._normalize(int(float(candle['timestamp'])), intervalo_ms)
        candle['timestamp'] = ts_norm
        self.total += 1

        if ts_norm in self._recent_set or any(c['timestamp'] == ts_norm for c in self._buffer):
            self.rejected += 1
            return [], 'duplicate', self._should_warn()

        if self.last_ts_processed and ts_norm < self.last_ts_processed:
            self.rejected += 1
            return [], 'out_of_order', self._should_warn()

        self._buffer.append({'timestamp': ts_norm, 'data': candle})
        self._buffer = deque(sorted(self._buffer, key=lambda x: x['timestamp']))

        ready: List[Dict] = []
        while self._buffer:
            if self.last_ts_processed is None:
                cand = self._buffer.popleft()
            else:
                expected = self.last_ts_processed + intervalo_ms
                if self._buffer[0]['timestamp'] == expected or len(self._buffer) >= self.buffer_size:
                    cand = self._buffer.popleft()
                else:
                    break
            ts_proc = cand['timestamp']
            ready.append(cand['data'])
            self.last_ts_processed = ts_proc
            self._recent.append(ts_proc)
            self._recent_set.add(ts_proc)
            if len(self._recent) > self.max_lru:
                old = self._recent.popleft()
                self._recent_set.discard(old)

        return ready, None, self._should_warn()