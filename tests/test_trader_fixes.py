"""Tests de regresión para fixes aplicados a core/trader/trader.py.

Bug-IDs cubiertos:
- TRADER-BG-TASKS-SUPPRESS-01: contextlib.suppress(Exception) no captura CancelledError
- TRADER-DF-SHALLOW-COPY-01:  df.copy(deep=False) comparte arrays numpy entre hilos
- TRADER-BLOCKING-NO-TIMEOUT-01: future.result() sin timeout bloquea indefinidamente
"""
from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import Future
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from core.trader_modular import Trader

from .factories import DummyConfig, DummySupervisor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_trader(**overrides: Any) -> Trader:
    config = DummyConfig(**overrides)

    async def _handler(_: dict) -> None:
        return None

    return Trader(config, candle_handler=_handler, supervisor=DummySupervisor())


def _make_df(rows: int = 5) -> pd.DataFrame:
    """DataFrame con columnas numéricas para verificar identidad de arrays."""
    rng = np.random.default_rng(42)
    df = pd.DataFrame({
        "close": rng.random(rows),
        "open":  rng.random(rows),
        "high":  rng.random(rows),
        "low":   rng.random(rows),
        "volume": rng.random(rows),
        "timestamp": list(range(rows)),
    })
    df.attrs["tf"] = "1m"
    return df


# ---------------------------------------------------------------------------
# TRADER-BG-TASKS-SUPPRESS-01
# cerrar() NO debe propagar CancelledError cuando cancela _bg_tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cerrar_bg_tasks_does_not_propagate_cancelled_error() -> None:
    """TRADER-BG-TASKS-SUPPRESS-01: cerrar() completa sin propagar CancelledError.

    Antes del fix, contextlib.suppress(Exception) no capturaba CancelledError
    (es BaseException en Python 3.8+) y la excepción se escapaba de cerrar().
    """
    trader = _build_trader()

    # Parchear stop() para que no haga nada
    trader.stop = AsyncMock()  # type: ignore[method-assign]
    # Garantizar que orders es None para evitar dependencias externas
    trader.orders = None  # type: ignore[assignment]
    trader.bus = None  # type: ignore[assignment]

    # Crear un task que, cuando se cancele, devuelve CancelledError
    async def _sleeper() -> None:
        await asyncio.sleep(10_000.0)

    sleepy_task = asyncio.create_task(_sleeper())
    trader._bg_tasks.add(sleepy_task)

    # Llamar cerrar() — debe completar sin propagar CancelledError
    try:
        await trader.cerrar()
    except asyncio.CancelledError:
        pytest.fail(
            "cerrar() propagó CancelledError — "
            "TRADER-BG-TASKS-SUPPRESS-01 no corregido"
        )

    # El task debe haberse cancelado
    assert sleepy_task.done(), "El bg_task no terminó tras cerrar()"
    assert sleepy_task.cancelled(), "El bg_task no fue cancelado"


@pytest.mark.asyncio
async def test_cerrar_bg_tasks_suppress_regular_exception() -> None:
    """cerrar() tampoco propaga Exception ordinaria desde un bg_task."""
    trader = _build_trader()
    trader.stop = AsyncMock()  # type: ignore[method-assign]
    trader.orders = None  # type: ignore[assignment]
    trader.bus = None  # type: ignore[assignment]

    async def _faulty() -> None:
        raise RuntimeError("error de test")

    faulty_task = asyncio.create_task(_faulty())
    # Dejar que el task falle de forma natural antes de añadirlo
    with contextlib.suppress(RuntimeError, asyncio.CancelledError, Exception):
        await faulty_task

    trader._bg_tasks.add(faulty_task)

    # cerrar() debe ignorar el task ya terminado (done() → continue)
    await trader.cerrar()
    assert faulty_task.done()


# ---------------------------------------------------------------------------
# TRADER-DF-SHALLOW-COPY-01
# _run_eval_offloaded usa df.copy() deep=True → arrays numpy aislados
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_eval_offloaded_uses_deep_copy() -> None:
    """TRADER-DF-SHALLOW-COPY-01: el worker recibe un df con arrays independientes.

    Con deep=False, df_copy.values y df.values compartirían la misma memoria
    numpy. Verificamos que los buffers de datos subyacentes sean distintos objetos.
    """
    trader = _build_trader()

    original_df = _make_df()
    original_close_buf = original_df["close"].values  # referencia al buffer numpy

    received_buffers: list[Any] = []

    async def _fake_pipeline(
        self_: Any,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        on_event: Any,
    ) -> None:
        received_buffers.append(df["close"].values)
        return None

    estado_dummy = None

    # Parchear TraderLite._execute_pipeline para capturar el df recibido
    with patch(
        "core.trader.trader_lite.TraderLite._execute_pipeline",
        new=_fake_pipeline,
    ):
        # Desactivar offload al worker loop para que el fallback inline se use
        # (simplifica el test: evita lanzar hilo dedicado)
        trader._eval_offload_enabled = False
        # Forzar shallow path manualmente: llamar _run_eval_offloaded que hace
        # df_copy = df.copy() y luego fallback porque _submit_eval_offload devuelve None
        with patch.object(trader, "_submit_eval_offload", return_value=None):
            await trader._run_eval_offloaded("BTCUSDT", original_df, estado_dummy, None)

    assert received_buffers, "El pipeline nunca fue invocado"
    received_buf = received_buffers[0]

    # [TRADER-DF-SHALLOW-COPY-01] Con deep=True, los buffers deben ser objetos distintos
    assert received_buf is not original_close_buf, (
        "df.copy(deep=False) fue usado — el array 'close' en el worker comparte "
        "memoria con el DataFrame original. TRADER-DF-SHALLOW-COPY-01 no corregido."
    )
    # Los valores deben ser idénticos (copia, no modificación)
    assert np.array_equal(received_buf, original_close_buf), (
        "La copia profunda alteró los valores — inesperado"
    )


# ---------------------------------------------------------------------------
# TRADER-BLOCKING-NO-TIMEOUT-01
# _blocking_verificar_entrada llama future.result(timeout=60.0)
# ---------------------------------------------------------------------------


def test_blocking_verificar_entrada_calls_result_with_timeout() -> None:
    """TRADER-BLOCKING-NO-TIMEOUT-01: future.result se llama con timeout=60.0.

    Antes del fix, future.result() sin timeout podía bloquear el hilo llamante
    indefinidamente si el worker loop se colgaba.
    """
    trader = _build_trader()

    result_calls: list[dict] = []

    mock_future: MagicMock = MagicMock(spec=Future)

    def _tracked_result(timeout: float | None = None) -> None:
        result_calls.append({"timeout": timeout})
        return None

    mock_future.result.side_effect = _tracked_result

    with patch.object(trader, "_submit_eval_offload", return_value=mock_future):
        original_df = _make_df()
        trader._blocking_verificar_entrada("BTCUSDT", original_df, None, None)

    assert result_calls, "_blocking_verificar_entrada nunca llamó future.result()"
    call = result_calls[0]
    assert call["timeout"] == 60.0, (
        f"future.result() fue llamado con timeout={call['timeout']!r} en vez de 60.0. "
        "TRADER-BLOCKING-NO-TIMEOUT-01 no corregido."
    )
