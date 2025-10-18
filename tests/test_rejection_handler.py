from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import List

from types import SimpleNamespace

import pandas as pd
import pytest

from core.rejection_handler import RejectionHandler


def _rechazos_csv(log_dir: Path) -> List[Path]:
    carpeta = log_dir / 'rechazos'
    if not carpeta.exists():
        return []
    return sorted(carpeta.glob('*.csv'))


def test_flush_retries_recovers(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    handler = RejectionHandler(
        log_dir=str(log_dir),
        batch_size=1,
        flush_retry_attempts=3,
        flush_retry_base_delay=0.0,
        flush_retry_max_delay=0.0,
    )

    original_to_csv = pd.DataFrame.to_csv
    llamadas = {'total': 0}

    def flaky_to_csv(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        llamadas['total'] += 1
        if llamadas['total'] < 2:
            raise OSError('disk busy')
        return original_to_csv(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, 'to_csv', flaky_to_csv)

    handler.registrar('BTCUSDT', 'test retry')
    handler.flush()

    archivos = _rechazos_csv(log_dir)
    assert len(archivos) == 1
    df = pd.read_csv(archivos[0])
    assert df['symbol'].tolist() == ['BTCUSDT']
    assert llamadas['total'] == 2
    assert handler._fallback_queue == []


def test_flush_fallback_on_persistent_failure(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    fallback_batches: List[List[dict]] = []
    handler = RejectionHandler(
        log_dir=str(log_dir),
        batch_size=1,
        flush_retry_attempts=2,
        flush_retry_base_delay=0.0,
        flush_retry_max_delay=0.0,
        fallback_handler=lambda payload: fallback_batches.append(payload),
    )

    original_to_csv = pd.DataFrame.to_csv

    def failing_to_csv(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError('disk full')

    monkeypatch.setattr(pd.DataFrame, 'to_csv', failing_to_csv)

    handler.registrar('ETHUSDT', 'disk error')
    handler.flush()

    assert fallback_batches and fallback_batches[-1][0]['symbol'] == 'ETHUSDT'
    assert _rechazos_csv(log_dir) == []

    monkeypatch.setattr(pd.DataFrame, 'to_csv', original_to_csv)

    handler.flush()

    archivos = _rechazos_csv(log_dir)
    assert len(archivos) == 1
    df = pd.read_csv(archivos[0])
    assert df['symbol'].tolist() == ['ETHUSDT']
    assert handler._fallback_queue == []


@pytest.mark.asyncio
async def test_flush_periodically_survives_exceptions(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    handler = RejectionHandler(log_dir=str(log_dir), batch_size=1)

    call_sequence: List[str] = []

    def flaky_flush() -> None:
        call_sequence.append('call')
        if len(call_sequence) == 1:
            raise RuntimeError('boom')

    monkeypatch.setattr(handler, 'flush', flaky_flush)

    ticks: List[str] = []
    monkeypatch.setattr('core.rejection_handler.tick', lambda name: ticks.append(name))

    stop_event = asyncio.Event()
    task = asyncio.create_task(handler.flush_periodically(0.01, stop_event))

    while len(call_sequence) < 2:
        await asyncio.sleep(0.005)

    stop_event.set()
    await asyncio.sleep(0.02)
    await task

    assert handler._flush_failures == 1
    assert call_sequence == ['call', 'call', 'call']
    assert ticks == ['rechazos_flush', 'rechazos_flush']


def test_registrar_no_falla_si_metricas_rompen(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    handler = RejectionHandler(log_dir=str(log_dir), batch_size=5)

    eventos_auditoria: list[dict] = []

    def dummy_auditoria(**kwargs):  # type: ignore[no-untyped-def]
        eventos_auditoria.append(kwargs)

    def boom_metrics(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError('metrics offline')

    monkeypatch.setattr('core.rejection_handler.registrar_auditoria', dummy_auditoria)
    monkeypatch.setattr(
        'core.rejection_handler.registro_metrico',
        SimpleNamespace(registrar=boom_metrics),
    )

    handler.registrar('BTCUSDT', 'risk check failed', puntaje=0.1, peso_total=1.0)

    assert len(handler._buffer) == 1
    assert eventos_auditoria and eventos_auditoria[-1]['symbol'] == 'BTCUSDT'


def test_registrar_auditoria_reintenta_en_caso_de_error(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    handler = RejectionHandler(
        log_dir=str(log_dir),
        batch_size=5,
        audit_retry_attempts=3,
        audit_retry_base_delay=0.0,
        audit_retry_max_delay=0.0,
    )

    llamadas = {'total': 0}

    def flaky_auditoria(**kwargs):  # type: ignore[no-untyped-def]
        llamadas['total'] += 1
        if llamadas['total'] < 2:
            raise RuntimeError('auditoria temporalmente indisponible')

    monkeypatch.setattr('core.rejection_handler.registrar_auditoria', flaky_auditoria)
    monkeypatch.setattr(handler, '_sleep', lambda *_args, **_kwargs: None)

    handler.registrar('BTCUSDT', 'retry audit path', puntaje=0.2, peso_total=1.0)

    assert llamadas['total'] == 2
    assert handler._audit_failures_streak == 0
    assert handler._audit_dead_letter == []


def test_circuit_breaker_auditoria(tmp_path: Path, monkeypatch) -> None:
    log_dir = tmp_path / 'logs'
    handler = RejectionHandler(
        log_dir=str(log_dir),
        batch_size=5,
        audit_retry_attempts=1,
        audit_retry_base_delay=0.0,
        audit_retry_max_delay=0.0,
        audit_circuit_breaker_threshold=1,
        audit_circuit_breaker_reset=60.0,
    )

    intentos = {'total': 0}

    def failing_auditoria(**kwargs):  # type: ignore[no-untyped-def]
        intentos['total'] += 1
        raise RuntimeError('auditoria fuera de servicio')

    monkeypatch.setattr('core.rejection_handler.registrar_auditoria', failing_auditoria)
    monkeypatch.setattr(handler, '_sleep', lambda *_args, **_kwargs: None)

    handler.registrar('BTCUSDT', 'primer fallo', puntaje=0.0, peso_total=1.0)

    assert intentos['total'] == 1
    assert handler._audit_failures_streak == 1
    assert handler._audit_dead_letter and handler._audit_dead_letter[-1]['symbol'] == 'BTCUSDT'
    assert handler._audit_circuit_breaker_open_until is not None

    handler.registrar('ETHUSDT', 'circuit abierto', puntaje=0.0, peso_total=1.0)

    assert intentos['total'] == 1
    assert handler._audit_dead_letter[-1]['symbol'] == 'ETHUSDT'

    handler._audit_circuit_breaker_open_until = time.monotonic() - 1

    auditoria_exitos = []

    def success_auditoria(**kwargs):  # type: ignore[no-untyped-def]
        auditoria_exitos.append(kwargs['symbol'])

    monkeypatch.setattr('core.rejection_handler.registrar_auditoria', success_auditoria)

    handler.reenviar_auditorias_pendientes()

    assert set(auditoria_exitos) == {'BTCUSDT', 'ETHUSDT'}
    assert handler._audit_dead_letter == []