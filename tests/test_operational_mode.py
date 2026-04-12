"""Pruebas aisladas del servicio de modo operativo (sin conftest pesado)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.operational_mode import (
    OperationalMode,
    OperationalModeRepository,
    OperationalModeService,
    _hashes_equal_timing_safe,
    _hash_token,
)


def test_hashes_equal_timing_safe_rejects_length_mismatch() -> None:
    assert not _hashes_equal_timing_safe("a" * 64, "b" * 32)
    assert not _hashes_equal_timing_safe("", "abc")


def test_hashes_equal_timing_safe_accepts_valid_sha256_hex() -> None:
    token = "secret"
    digest = _hash_token(token)
    assert _hashes_equal_timing_safe(digest, digest.upper())
    assert not _hashes_equal_timing_safe(digest, _hash_token("other"))


@dataclass
class _Cfg:
    api_key: str | None = "k"
    api_secret: str | None = "s"
    modo_real: bool = True
    modo_operativo: OperationalMode = OperationalMode.REAL
    umbral_riesgo_diario: float = 0.05


@pytest.mark.asyncio
async def test_apply_mode_updates_order_manager(tmp_path: Any) -> None:
    repo = OperationalModeRepository(db_path=tmp_path / "op.db")
    cfg = _Cfg(
        modo_real=False,
        modo_operativo=OperationalMode.PAPER_TRADING,
        umbral_riesgo_diario=0.05,
    )
    trader = MagicMock()
    trader.orders = MagicMock()
    trader.orders.modo_real = False
    trader.orders._sync_task = None

    svc = OperationalModeService(
        config=cfg,
        trader=trader,
        event_bus=None,
        repository=repo,
        poll_interval=3600.0,
    )
    svc._expected_token_hash = _hash_token("ok")
    svc._current_mode = OperationalMode.PAPER_TRADING

    from core.operational_mode import ModeCommand

    cmd = ModeCommand(
        command_id=1,
        mode=OperationalMode.REAL,
        token_hash=_hash_token("ok"),
        actor="test",
        reason=None,
        requested_at=0.0,
    )
    await svc._handle_command(cmd)

    assert trader.modo_real is True
    assert trader.orders.modo_real is True
    assert trader.orders._config.modo_real is True
    trader.orders.start_sync.assert_called_once()
