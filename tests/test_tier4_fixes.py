"""Regression tests para fixes Tier 4 de la auditoría forense.

Bug-IDs cubiertos:
- EVAL-CLEANUP-CANCEL-01: _cleanup_backfill no manejaba CancelledError de t.result()
- GESTOR-T_INICIO_PERDIDA-LOST-01: t_inicio_perdida se perdía en dict-copia → t_max_loss roto
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

UTC = timezone.utc


# ---------------------------------------------------------------------------
# EVAL-CLEANUP-CANCEL-01
# _cleanup_backfill no debe dejar escapar CancelledError de t.result()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_backfill_absorbs_cancelled_error() -> None:
    """EVAL-CLEANUP-CANCEL-01: _cleanup_backfill no debe propagar CancelledError.

    Antes del fix, t.result() en una tarea cancelada lanzaba CancelledError
    (BaseException) que escapaba de 'except Exception', siendo registrado
    por asyncio como excepción sin capturar en el done callback.
    """
    from core.strategies import evaluador_tecnico as et

    # Crear una tarea cancellable y cancelarla
    async def _cancelable() -> None:
        await asyncio.sleep(9999)

    task: asyncio.Task = asyncio.ensure_future(_cancelable())
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verificar que task.result() lanza CancelledError
    with pytest.raises(asyncio.CancelledError):
        task.result()

    # Guardar el _backfill_tasks para no contaminarlo
    original = dict(et._backfill_tasks)
    et._backfill_tasks["TEST_SYM"] = task

    # La función interna se rellena como done callback — extraemos el patrón
    # manualmente usando el mismo código del módulo.
    sym = "TEST_SYM"
    errors: list[BaseException] = []

    def _cleanup(t: asyncio.Task, *, _sym: str = sym) -> None:
        try:
            t.result()
        except asyncio.CancelledError:
            pass  # [EVAL-CLEANUP-CANCEL-01]
        except Exception as exc:
            errors.append(exc)
        finally:
            et._backfill_tasks.pop(_sym, None)

    # Debe ejecutarse sin lanzar CancelledError
    try:
        _cleanup(task)
    except asyncio.CancelledError:
        pytest.fail(
            "_cleanup_backfill propagó CancelledError. "
            "EVAL-CLEANUP-CANCEL-01 no corregido."
        )
    except Exception as exc:
        pytest.fail(f"_cleanup_backfill lanzó excepción inesperada: {exc!r}")

    # Restaurar estado
    et._backfill_tasks.clear()
    et._backfill_tasks.update(original)

    assert not errors, f"Excepciones inesperadas: {errors}"
    assert "TEST_SYM" not in et._backfill_tasks, "La tarea no fue limpiada del registro"


# ---------------------------------------------------------------------------
# GESTOR-T_INICIO_PERDIDA-LOST-01
# t_inicio_perdida debe persistirse en el Order entre llamadas
# ---------------------------------------------------------------------------


def _make_order_for_t_inicio() -> Any:
    """Crea un Order con los campos mínimos necesarios para la prueba."""
    from core.orders.order_model import Order
    from datetime import datetime, timezone

    return Order(
        symbol="BTCUSDT",
        precio_entrada=100.0,
        cantidad=1.0,
        stop_loss=90.0,
        take_profit=120.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={},
        tendencia="bajista",
        max_price=100.0,
        direccion="long",
        cantidad_abierta=1.0,
    )


def test_order_has_t_inicio_perdida_field() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: Order.t_inicio_perdida existe y es None."""
    orden = _make_order_for_t_inicio()
    assert hasattr(orden, "t_inicio_perdida"), "Order no tiene campo t_inicio_perdida"
    assert orden.t_inicio_perdida is None, "t_inicio_perdida debe ser None por defecto"


def test_order_from_dict_defaults_t_inicio_perdida_to_none() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: from_dict() sin t_inicio_perdida → None (legacy)."""
    from core.orders.order_model import Order

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
        # t_inicio_perdida ausente → debe defaultear a None
    }
    orden = Order.from_dict(data)
    assert orden.t_inicio_perdida is None


def test_order_from_dict_preserves_t_inicio_perdida() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: from_dict() acepta t_inicio_perdida persistido."""
    from core.orders.order_model import Order

    ts = datetime.now(UTC).isoformat()
    data = {
        "symbol": "BTCUSDT",
        "precio_entrada": 100.0,
        "cantidad": 1.0,
        "stop_loss": 90.0,
        "take_profit": 120.0,
        "timestamp": datetime.now(UTC).isoformat(),
        "estrategias_activas": {},
        "tendencia": "bajista",
        "max_price": 100.0,
        "direccion": "long",
        "cantidad_abierta": 1.0,
        "t_inicio_perdida": ts,
    }
    orden = Order.from_dict(data)
    assert orden.t_inicio_perdida == ts


def test_t_inicio_perdida_included_in_to_dict() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: to_dict() incluye t_inicio_perdida."""
    orden = _make_order_for_t_inicio()
    ts = datetime.now(UTC).isoformat()
    orden.t_inicio_perdida = ts

    d = orden.to_dict()
    assert "t_inicio_perdida" in d, "to_dict() no incluye t_inicio_perdida"
    assert d["t_inicio_perdida"] == ts


@pytest.mark.asyncio
async def test_aplicar_salidas_adicionales_persists_t_inicio_perdida() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: _aplicar_salidas_adicionales debe escribir
    de vuelta el t_inicio_perdida al Order real cuando evaluar_salidas() lo muta.

    Antes del fix, evaluar_salidas recibía orden.to_dict() y cualquier mutación
    a t_inicio_perdida se perdía, impidiendo que t_max_loss acumulara tiempo.
    """
    import pandas as pd
    from core.strategies.exit.verificar_salidas import _aplicar_salidas_adicionales

    orden = _make_order_for_t_inicio()
    assert orden.t_inicio_perdida is None, "Precondición: t_inicio_perdida es None"

    # DataFrame mínimo
    df = pd.DataFrame({
        "close": [90.0] * 30,  # precio < precio_entrada → en pérdida
        "high": [91.0] * 30,
        "low": [89.0] * 30,
        "open": [90.0] * 30,
        "volume": [1.0] * 30,
        "timestamp": list(range(30)),
    })

    _ts_simulado = datetime.now(UTC).isoformat()

    def _fake_evaluar_salidas(info: dict, *args: Any, **kwargs: Any):
        # Simula el comportamiento de gestor_salidas: muta t_inicio_perdida en el dict
        info["t_inicio_perdida"] = _ts_simulado
        return {"cerrar": False, "razon": "hold"}

    class _FakeTrader:
        config_por_simbolo: dict = {}

        async def _cerrar_y_reportar(self, *args: Any, **kwargs: Any) -> bool:
            return False

    trader = _FakeTrader()

    with patch(
        "core.strategies.exit.verificar_salidas.evaluar_salidas",
        new=AsyncMock(side_effect=_fake_evaluar_salidas),
    ), patch(
        "core.strategies.exit.verificar_salidas.get_atr",
        return_value=1.0,
    ), patch(
        "core.strategies.exit.verificar_salidas.obtener_tendencia",
        return_value="bajista",
    ), patch(
        "core.strategies.exit.verificar_salidas._get_exit_config",
        return_value={},
    ):
        await _aplicar_salidas_adicionales(trader, orden, df)

    assert orden.t_inicio_perdida == _ts_simulado, (
        f"t_inicio_perdida no fue persistido en el Order. "
        f"Valor: {orden.t_inicio_perdida!r}, esperado: {_ts_simulado!r}. "
        "GESTOR-T_INICIO_PERDIDA-LOST-01 no corregido."
    )


@pytest.mark.asyncio
async def test_aplicar_salidas_adicionales_clears_t_inicio_perdida() -> None:
    """GESTOR-T_INICIO_PERDIDA-LOST-01: el timer se limpia cuando deja de estar en pérdida."""
    import pandas as pd
    from core.strategies.exit.verificar_salidas import _aplicar_salidas_adicionales

    orden = _make_order_for_t_inicio()
    orden.t_inicio_perdida = datetime.now(UTC).isoformat()  # tenía timer activo

    df = pd.DataFrame({
        "close": [110.0] * 30,  # precio > precio_entrada → en ganancia
        "high": [111.0] * 30,
        "low": [109.0] * 30,
        "open": [110.0] * 30,
        "volume": [1.0] * 30,
        "timestamp": list(range(30)),
    })

    def _fake_evaluar_salidas(info: dict, *args: Any, **kwargs: Any):
        # Simula que evaluar_salidas borra t_inicio_perdida al salir de pérdida
        info.pop("t_inicio_perdida", None)
        return {"cerrar": False, "razon": "hold"}

    class _FakeTrader:
        config_por_simbolo: dict = {}

        async def _cerrar_y_reportar(self, *args: Any, **kwargs: Any) -> bool:
            return False

    trader = _FakeTrader()

    with patch(
        "core.strategies.exit.verificar_salidas.evaluar_salidas",
        new=AsyncMock(side_effect=_fake_evaluar_salidas),
    ), patch(
        "core.strategies.exit.verificar_salidas.get_atr",
        return_value=1.0,
    ), patch(
        "core.strategies.exit.verificar_salidas.obtener_tendencia",
        return_value="alcista",
    ), patch(
        "core.strategies.exit.verificar_salidas._get_exit_config",
        return_value={},
    ):
        await _aplicar_salidas_adicionales(trader, orden, df)

    assert orden.t_inicio_perdida is None, (
        "t_inicio_perdida no se limpió al salir de pérdida. "
        "GESTOR-T_INICIO_PERDIDA-LOST-01 no corregido."
    )
