"""Funciones de backfill reutilizadas por ``TraderLite``.

Este módulo aísla toda la lógica relacionada con el servicio de backfill para
mantener ``trader_lite`` más legible y fácil de mantener.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from binance_api.cliente import fetch_ohlcv_async
from core.backfill_service import BackfillService
from core.metrics import (
    BACKFILL_DURATION_SECONDS,
    BACKFILL_GAPS_FOUND_TOTAL,
    BACKFILL_KLINES_FETCHED_TOTAL,
    BACKFILL_REQUESTS_TOTAL,
    BUFFER_SIZE_V2,
)
from core.procesar_vela import get_buffer_manager
from core.utils.timeframes import tf_to_ms
from core.utils.utils import configurar_logger


log = configurar_logger("trader_modular", modo_silencioso=True)


class TraderLiteBackfillMixin:
    """Mezcla con utilidades de backfill para ``TraderLite``."""

    _backfill_service: BackfillService | None
    _backfill_enabled: bool
    _backfill_mode: str
    _backfill_task: Optional[asyncio.Task]
    _backfill_min_needed: int
    _backfill_warmup_extra: int
    _backfill_headroom: int
    _buffer_manager: Any | None
    _backfill_ready_flags: Dict[tuple[str, str], bool]
    _cliente: Any

    def _init_backfill_service(self) -> None:
        if not self._backfill_enabled:
            return

        try:
            metrics = SimpleNamespace(
                backfill_requests_total=BACKFILL_REQUESTS_TOTAL,
                backfill_klines_fetched_total=BACKFILL_KLINES_FETCHED_TOTAL,
                backfill_duration_seconds=BACKFILL_DURATION_SECONDS,
                backfill_gaps_found_total=BACKFILL_GAPS_FOUND_TOTAL,
                buffer_size_v2=BUFFER_SIZE_V2,
            )
            self._backfill_service = BackfillService(
                fetch_klines=self._fetch_klines_for_backfill,
                buffer_append_many=self._buffer_append_many,
                buffer_get=self._buffer_get,
                set_ready_flag=self._backfill_set_ready,
                logger=configurar_logger("backfill_service"),
                metrics=metrics,
                headroom=self._backfill_headroom,
            )
        except Exception:
            self._backfill_service = None
            self._backfill_enabled = False
            log.exception("No se pudo inicializar BackfillService; se deshabilita")

    async def start_backfill(self) -> None:
        if not self._backfill_service:
            return

        symbols = [s.upper() for s in self.config.symbols]
        try:
            if self._backfill_mode == "B":
                if self._backfill_task and not self._backfill_task.done():
                    return
                self._backfill_task = asyncio.create_task(
                    self._backfill_service.run(
                        symbols,
                        self.config.intervalo_velas,
                        self._backfill_min_needed,
                        self._backfill_warmup_extra,
                        mode="B",
                    ),
                    name="backfill_service",
                )
                self._backfill_task.add_done_callback(self._backfill_task_done)
            else:
                await self._backfill_service.run(
                    symbols,
                    self.config.intervalo_velas,
                    self._backfill_min_needed,
                    self._backfill_warmup_extra,
                    mode="A",
                )
        except Exception:
            log.exception("Error ejecutando backfill inicial")
            raise

    def _backfill_task_done(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception:
            log.exception("Backfill en paralelo finalizó con error")

    def _backfill_set_ready(self, symbol: str, timeframe: str, ready: bool) -> None:
        key = (symbol.upper(), timeframe.lower())
        self._backfill_ready_flags[key] = bool(ready)

    def is_symbol_ready(self, symbol: str, timeframe: Optional[str] = None) -> bool:
        if not self._backfill_service or not self._backfill_enabled:
            return True
        if not self._backfill_ready_flags and self._backfill_mode != "B":
            return True
        tf = timeframe or getattr(self.config, "intervalo_velas", "")
        if not tf or tf == "unknown":
            tf = getattr(self.config, "intervalo_velas", "")
        key = (symbol.upper(), str(tf).lower())
        if key in self._backfill_ready_flags:
            return self._backfill_ready_flags[key]
        return self._backfill_service.is_ready(symbol, str(tf))

    def _buffer_append_many(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> None:
        manager = self._get_buffer_manager()
        manager.extend(symbol, candles)

    def _buffer_get(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        manager = self._get_buffer_manager()
        return manager.snapshot(symbol)

    def _get_buffer_manager(self):
        if self._buffer_manager is None:
            self._buffer_manager = get_buffer_manager()
        return self._buffer_manager

    async def _fetch_klines_for_backfill(
        self,
        symbol: str,
        timeframe: str,
        start: int,
        end: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        since = int(start)
        tf_ms = tf_to_ms(timeframe)
        raw = await fetch_ohlcv_async(self._cliente, symbol, timeframe, since=since, limit=limit)
        max_open = end - tf_ms
        result: List[Dict[str, Any]] = []
        for entry in raw:
            if isinstance(entry, dict):
                open_time = entry.get("open_time") or entry.get("openTime") or entry.get("timestamp")
                open_price = entry.get("open") or entry.get("o")
                high_price = entry.get("high") or entry.get("h")
                low_price = entry.get("low") or entry.get("l")
                close_price = entry.get("close") or entry.get("c")
                volume = entry.get("volume") or entry.get("v")
            else:
                if not entry or len(entry) < 6:
                    continue
                open_time, open_price, high_price, low_price, close_price, volume = entry[:6]
            try:
                open_ts = int(float(open_time))
            except (TypeError, ValueError):
                continue
            if open_ts < start or open_ts > max_open:
                continue
            result.append(
                {
                    "symbol": symbol.upper(),
                    "timeframe": timeframe,
                    "open_time": open_ts,
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": float(volume),
                    "close_time": open_ts + tf_ms - 1,
                }
            )
        return result
