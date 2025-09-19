from __future__ import annotations

import cProfile
import json
import os
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pstats

from .logger import configurar_logger


log = configurar_logger('candle_profiler', modo_silencioso=True)

_STAGE_CATEGORIES: dict[str, str] = {
    'state_update': 'cpu',
    'indicators': 'cpu',
    'trend': 'cpu',
    'strategy_engine': 'cpu',
    'risk_checks': 'cpu',
    'spread_guard': 'cpu',
    'position_manager': 'io',
    'notifications_io': 'io',
    'persistence_io': 'io',
    'other': 'cpu',
}


class CandleProfiler:
    """Mide tiempos por etapa y genera perfiles ``cProfile`` acumulados."""

    def __init__(self, symbol: str, timeframe: str, enabled: bool) -> None:
        self._enabled = enabled
        self._profile = cProfile.Profile() if enabled else None
        self._stages: dict[str, float] = defaultdict(float)
        self._symbol = symbol
        self._timeframe = timeframe

    @contextmanager
    def stage(self, name: str) -> Iterable[None]:
        start = time.perf_counter()
        if self._profile:
            self._profile.enable()
        try:
            yield
        finally:
            if self._profile:
                self._profile.disable()
            self._stages[name] += (time.perf_counter() - start) * 1000

    def finalize(self, candle_ts: int | float | None = None) -> None:
        if not self._enabled:
            return
        repository = _ProfilerRepository.get()
        repository.consume(
            (self._symbol, self._timeframe),
            self._profile,
            dict(self._stages),
            candle_ts=candle_ts,
        )


class _ProfilerRepository:
    _instance: '_ProfilerRepository | None' = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._stats: dict[Tuple[str, str], pstats.Stats] = {}
        self._stage_totals: dict[Tuple[str, str], dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        self._counts: dict[Tuple[str, str], int] = defaultdict(int)
        self._output_dir = Path(os.getenv('TRADER_PROFILE_DIR', 'profiling'))
        self._output_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get(cls) -> '_ProfilerRepository':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def consume(
        self,
        key: Tuple[str, str],
        profile: cProfile.Profile | None,
        stages: Dict[str, float],
        *,
        candle_ts: int | float | None,
    ) -> None:
        if profile is None:
            return
        stats = pstats.Stats(profile)
        with self._lock:
            current = self._stats.get(key)
            if current is None:
                self._stats[key] = stats
            else:
                current.add(stats)
            stage_totals = self._stage_totals[key]
            for stage, value in stages.items():
                stage_totals[stage] += value
            self._counts[key] += 1

    def flush(self) -> None:
        with self._lock:
            for key, stats in self._stats.items():
                symbol, timeframe = key
                base = f"{symbol.replace('/', '_')}_{timeframe}"
                profile_path = self._output_dir / f"{base}.prof"
                stats.dump_stats(str(profile_path))
                stage_totals = self._stage_totals.get(key, {})
                total_ms = sum(stage_totals.values())
                summary = {
                    'calls': self._counts.get(key, 0),
                    'total_ms': total_ms,
                    'stages_ms': {k: round(v, 3) for k, v in stage_totals.items()},
                }
                summary_path = self._output_dir / f"{base}_stages.json"
                summary_path.write_text(
                    json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8'
                )
                flamegraph = _build_flamegraph(stage_totals)
                flame_path = self._output_dir / f"{base}_flamegraph.json"
                flame_path.write_text(
                    json.dumps(flamegraph, indent=2, sort_keys=True), encoding='utf-8'
                )
            self._stats.clear()
            self._stage_totals.clear()
            self._counts.clear()


def _build_flamegraph(stage_totals: Dict[str, float]) -> Dict[str, Any]:
    total_ms = sum(stage_totals.values())
    root: Dict[str, Any] = {'name': 'procesar_vela', 'value': round(total_ms, 3)}
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    totals: dict[str, float] = defaultdict(float)
    for stage, value in stage_totals.items():
        category = _STAGE_CATEGORIES.get(stage, 'cpu')
        totals[category] += value
        buckets[category].append(
            {'name': stage, 'value': round(value, 3)}
        )
    children = []
    for category, stages in buckets.items():
        children.append(
            {
                'name': category,
                'value': round(totals[category], 3),
                'children': stages,
            }
        )
    if children:
        root['children'] = children
    return root


def flush_profiles() -> None:
    """Exporta perfiles acumulados a disco."""

    repository = _ProfilerRepository.get()
    repository.flush()