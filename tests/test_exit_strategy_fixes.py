"""Regression tests for exit strategy fixes.

Bug-IDs cubiertos:
- VERIF-CONCURRENT-HIERARCHY-01: asyncio.gather() corría SL/TP/macro en paralelo
- VERIF-LOAD-REDUNDANT-01: load_exit_config() sin cache en hot path
- VERIF-TRAILING-STATE-LOST-01: sl_trailing no persistía entre velas
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from core.orders.order_model import Order
from datetime import datetime, timezone

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_order(symbol: str = "BTCUSDT") -> Order:
    return Order(
        symbol=symbol,
        precio_entrada=100.0,
        cantidad=1.0,
        stop_loss=90.0,
        take_profit=120.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={},
        tendencia="alcista",
        max_price=100.0,
        direccion="long",
        cantidad_abierta=1.0,
    )


def _make_df(close: float = 95.0, rows: int = 30) -> pd.DataFrame:
    return pd.DataFrame({
        "close": [close] * rows,
        "high": [close + 1.0] * rows,
        "low": [close - 1.0] * rows,
        "open": [close] * rows,
        "volume": [1.0] * rows,
        "timestamp": list(range(rows)),
    })


# ---------------------------------------------------------------------------
# VERIF-CONCURRENT-HIERARCHY-01
# Los checks primarios deben ejecutarse secuencialmente respetando jerarquía.
# Si macro cierra la posición, SL y TP NO deben invocarse.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_macro_close_skips_sl_and_tp() -> None:
    """VERIF-CONCURRENT-HIERARCHY-01: si macro cierra, SL y TP no se llaman.

    Antes del fix, asyncio.gather() corría los tres en paralelo y todos
    completaban incluso si macro ya había cerrado la posición.
    """
    from core.strategies.exit.verificar_salidas import _run_checks_sequential

    calls: list[str] = []

    async def macro() -> bool:
        calls.append("macro")
        return True  # cierra

    async def sl() -> bool:
        calls.append("sl")
        return False

    async def tp() -> bool:
        calls.append("tp")
        return False

    action, closed = await _run_checks_sequential(
        [lambda: macro(), lambda: sl(), lambda: tp()],
        timeout=5.0,
        label="test",
        symbol="BTCUSDT",
    )

    assert closed is True
    assert action == "exit_closed"
    # [VERIF-CONCURRENT-HIERARCHY-01] macro cerró → SL y TP NO deben haberse llamado
    assert "sl" not in calls, (
        "SL fue invocado después de que macro cerró la posición. "
        "VERIF-CONCURRENT-HIERARCHY-01 no corregido."
    )
    assert "tp" not in calls, (
        "TP fue invocado después de que macro cerró la posición. "
        "VERIF-CONCURRENT-HIERARCHY-01 no corregido."
    )


@pytest.mark.asyncio
async def test_sl_close_skips_tp() -> None:
    """VERIF-CONCURRENT-HIERARCHY-01: si SL cierra, TP no se llama."""
    from core.strategies.exit.verificar_salidas import _run_checks_sequential

    calls: list[str] = []

    async def macro() -> bool:
        calls.append("macro")
        return False  # no cierra

    async def sl() -> bool:
        calls.append("sl")
        return True  # cierra

    async def tp() -> bool:
        calls.append("tp")
        return False

    _, closed = await _run_checks_sequential(
        [lambda: macro(), lambda: sl(), lambda: tp()],
        timeout=5.0,
        label="test",
        symbol="BTCUSDT",
    )

    assert closed is True
    assert "macro" in calls
    assert "sl" in calls
    assert "tp" not in calls, "TP fue invocado después de que SL cerró la posición."


@pytest.mark.asyncio
async def test_no_close_runs_all_checks() -> None:
    """Si ningún check cierra, todos deben ejecutarse (sin omitir)."""
    from core.strategies.exit.verificar_salidas import _run_checks_sequential

    calls: list[str] = []

    async def macro() -> bool:
        calls.append("macro")
        return False

    async def sl() -> bool:
        calls.append("sl")
        return False

    async def tp() -> bool:
        calls.append("tp")
        return False

    _, closed = await _run_checks_sequential(
        [lambda: macro(), lambda: sl(), lambda: tp()],
        timeout=5.0,
        label="test",
        symbol="BTCUSDT",
    )

    assert closed is False
    assert calls == ["macro", "sl", "tp"], "Todos los checks deben ejecutarse si nadie cierra"


@pytest.mark.asyncio
async def test_run_checks_sequential_timeout() -> None:
    """Un check que tarda más que timeout devuelve exit_timeout."""
    from core.strategies.exit.verificar_salidas import _run_checks_sequential

    async def slow() -> bool:
        await asyncio.sleep(10.0)
        return False

    action, closed = await _run_checks_sequential(
        [lambda: slow()],
        timeout=0.05,
        label="test",
        symbol="BTCUSDT",
    )

    assert action == "exit_timeout"
    assert closed is False


# ---------------------------------------------------------------------------
# VERIF-LOAD-REDUNDANT-01
# load_exit_config() NO debe llamarse en verificar_salidas directamente.
# ---------------------------------------------------------------------------


def test_get_exit_config_uses_cache() -> None:
    """VERIF-LOAD-REDUNDANT-01: _get_exit_config() lee JSON solo si no está en cache."""
    from core.strategies.exit.verificar_salidas import _get_exit_config

    class _FakeTrader:
        config_por_simbolo: dict = {}

    trader = _FakeTrader()
    load_calls: list[str] = []

    def fake_load(symbol: str) -> dict:
        load_calls.append(symbol)
        return {"timeout_validaciones": 5}

    with patch(
        "core.strategies.exit.verificar_salidas.load_exit_config",
        side_effect=fake_load,
    ):
        # Primera llamada → debe leer JSON
        cfg1 = _get_exit_config(trader, "BTCUSDT")
        assert load_calls == ["BTCUSDT"]

        # Segunda llamada con cache → NO debe releer JSON
        cfg2 = _get_exit_config(trader, "BTCUSDT")
        assert load_calls == ["BTCUSDT"], (
            "load_exit_config fue llamado de nuevo con cache activo. "
            "VERIF-LOAD-REDUNDANT-01 no corregido."
        )
        assert cfg1 is cfg2, "Debe devolverse la misma instancia del cache"


# ---------------------------------------------------------------------------
# VERIF-TRAILING-STATE-LOST-01
# sl_trailing debe persistirse en el Order entre llamadas
# ---------------------------------------------------------------------------


def test_order_has_sl_trailing_field() -> None:
    """VERIF-TRAILING-STATE-LOST-01: Order.sl_trailing existe y es None por defecto."""
    orden = _make_order()
    assert hasattr(orden, "sl_trailing"), "Order no tiene campo sl_trailing"
    assert orden.sl_trailing is None, "sl_trailing debe ser None por defecto"


def test_order_from_dict_preserves_sl_trailing() -> None:
    """VERIF-TRAILING-STATE-LOST-01: from_dict() acepta sl_trailing legado."""
    data = {
        "symbol": "BTCUSDT",
        "precio_entrada": 100.0,
        "cantidad": 1.0,
        "stop_loss": 90.0,
        "take_profit": 120.0,
        "timestamp": datetime.now(UTC).isoformat(),
        "estrategias_activas": {},
        "tendencia": "alcista",
        "max_price": 105.0,
        "direccion": "long",
        "cantidad_abierta": 1.0,
        "sl_trailing": 98.5,
    }
    orden = Order.from_dict(data)
    assert orden.sl_trailing == 98.5


def test_order_from_dict_defaults_sl_trailing_to_none() -> None:
    """VERIF-TRAILING-STATE-LOST-01: from_dict() sin sl_trailing → None (compatibilidad legacy)."""
    data = {
        "symbol": "BTCUSDT",
        "precio_entrada": 100.0,
        "cantidad": 1.0,
        "stop_loss": 90.0,
        "take_profit": 120.0,
        "timestamp": datetime.now(UTC).isoformat(),
        "estrategias_activas": {},
        "tendencia": "alcista",
        "max_price": 105.0,
        "direccion": "long",
        "cantidad_abierta": 1.0,
        # sl_trailing ausente → debe defaultear a None
    }
    orden = Order.from_dict(data)
    assert orden.sl_trailing is None


def test_sl_trailing_included_in_to_dict() -> None:
    """VERIF-TRAILING-STATE-LOST-01: to_dict() incluye sl_trailing para que
    verificar_trailing_stop() lo lea en la siguiente vela."""
    orden = _make_order()
    orden.sl_trailing = 97.0

    d = orden.to_dict()
    assert "sl_trailing" in d, "to_dict() no incluye sl_trailing"
    assert d["sl_trailing"] == 97.0


@pytest.mark.asyncio
async def test_manejar_trailing_stop_persists_sl_trailing() -> None:
    """VERIF-TRAILING-STATE-LOST-01: _manejar_trailing_stop persiste sl_trailing
    en el Order real después de que verificar_trailing_stop() lo compute.

    Antes del fix, sl_trailing se escribía solo al dict-copia y se perdía al
    retornar → guard de tick mínimo siempre bypaseado (sl_actual = None).
    """
    from core.strategies.exit.verificar_salidas import _manejar_trailing_stop

    orden = _make_order()
    orden.max_price = 110.0  # trailing armado por encima de entrada

    df = _make_df(close=108.0, rows=30)

    class _FakeTrader:
        config_por_simbolo: dict = {}

        async def _cerrar_y_reportar(self, *args: Any, **kwargs: Any) -> bool:
            return False

    trader = _FakeTrader()

    # Mock verificar_trailing_stop para que retorne no-cierre y escriba sl_trailing
    mock_sl = 105.0

    def _fake_verificar(info: dict, precio: float, df: Any, config: Any) -> tuple:
        # Simula escritura del sl_trailing al dict (comportamiento real)
        info["sl_trailing"] = mock_sl
        return False, "Trailing supervisando"

    with patch(
        "core.strategies.exit.verificar_salidas.verificar_trailing_stop",
        side_effect=_fake_verificar,
    ), patch(
        "core.strategies.exit.verificar_salidas.adaptar_configuracion",
        side_effect=lambda sym, df, cfg: cfg,
    ), patch(
        "core.strategies.exit.verificar_salidas._get_exit_config",
        return_value={"trailing_buffer": 0.005},
    ):
        await _manejar_trailing_stop(trader, orden, df)

    # [VERIF-TRAILING-STATE-LOST-01] sl_trailing debe haberse persistido en el Order
    assert orden.sl_trailing == mock_sl, (
        f"sl_trailing no fue persistido en el Order. "
        f"Valor: {orden.sl_trailing!r}, esperado: {mock_sl}. "
        "VERIF-TRAILING-STATE-LOST-01 no corregido."
    )
