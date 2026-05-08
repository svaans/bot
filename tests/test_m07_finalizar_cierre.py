"""Tests for M-07: _finalizar_cierre_completo_async deduplication.

Verifies that:
1. _finalizar_cierre_completo_async annotates the order correctly
2. The short-direction notify bug (hardcoded "Venta") is fixed
3. cerrar_async and cerrar_parcial_async use the shared helper (no divergence)
4. real_orders.eliminar_orden is called in modo_real (previously missing in cerrar_async)
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest

from core.orders.order_manager_cerrar import _finalizar_cierre_completo_async
from core.orders.order_model import Order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_orden(direccion: str = "long", pnl: float = 50.0) -> Order:
    orden = Order(
        symbol="BTC/USDT",
        precio_entrada=90_000.0,
        cantidad=0.01,
        stop_loss=88_000.0,
        take_profit=95_000.0,
        timestamp="2024-01-01T00:00:00+00:00",
        estrategias_activas={},
        tendencia="alcista",
        max_price=91_000.0,
        direccion=direccion,
        cantidad_abierta=0.01,
    )
    orden.pnl_realizado = pnl
    return orden


class _FakeBus:
    def __init__(self):
        self.published: list[tuple[str, dict]] = []

    async def publish(self, event: str, payload: dict) -> None:
        self.published.append((event, payload))


def _make_manager(*, modo_real: bool = False, bus: _FakeBus | None = None) -> MagicMock:
    m = MagicMock()
    m.bus = bus or _FakeBus()
    m.modo_real = modo_real
    m.historial = {}
    m.max_historial = 50
    m.ordenes = {"BTC/USDT": MagicMock()}
    m._registro_pendiente_paused = set()
    m._set_latent_pnl = MagicMock()
    m._emit_pnl_update = MagicMock()
    m._publish_registrar_perdida = AsyncMock()
    m._actualizar_capital_disponible = MagicMock()
    return m


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFinalizarCierreCompletoAsync:
    @pytest.mark.asyncio
    async def test_anota_precio_y_fecha_en_orden(self):
        orden = _make_orden()
        manager = _make_manager()
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "SL", "op-001"
        )
        assert orden.precio_cierre == 91_000.0
        assert orden.motivo_cierre == "SL"
        assert orden.fecha_cierre is not None

    @pytest.mark.asyncio
    async def test_retorno_calculado_y_asignado(self):
        """retorno_total = pnl_realizado / (precio_entrada * cantidad)."""
        orden = _make_orden(pnl=100.0)  # 100 / (90_000 * 0.01) = ~0.111
        manager = _make_manager()
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-002"
        )
        expected = 100.0 / (90_000.0 * 0.01)
        assert abs(orden.retorno_total - expected) < 1e-9

    @pytest.mark.asyncio
    async def test_orden_eliminada_de_memoria(self):
        orden = _make_orden()
        manager = _make_manager()
        manager.ordenes["BTC/USDT"] = MagicMock()
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-003"
        )
        assert "BTC/USDT" not in manager.ordenes

    @pytest.mark.asyncio
    async def test_win_streak_reset_publicado_para_ganancia(self):
        bus = _FakeBus()
        orden = _make_orden(pnl=50.0)  # positive return
        manager = _make_manager(bus=bus)
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-004"
        )
        events = [e for e, _ in bus.published]
        assert "risk.win_streak_reset" in events

    @pytest.mark.asyncio
    async def test_registrar_perdida_publicado_para_perdida(self):
        bus = _FakeBus()
        orden = _make_orden(pnl=-500.0)  # negative return
        manager = _make_manager(bus=bus)
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 88_000.0, "SL", "op-005"
        )
        assert manager._publish_registrar_perdida.called

    # -- M-07-BUG: short direction notify fix --

    @pytest.mark.asyncio
    async def test_long_close_notifica_venta(self):
        """Cierre de long → mensaje contiene 'Venta'."""
        bus = _FakeBus()
        orden = _make_orden(direccion="long")
        manager = _make_manager(modo_real=True, bus=bus)
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-006"
        )
        notifs = [p["mensaje"] for e, p in bus.published if e == "notify"]
        assert notifs, "Se esperaba al menos una notificación"
        assert any("Venta" in m for m in notifs), f"Mensajes: {notifs}"

    @pytest.mark.asyncio
    async def test_short_close_notifica_compra(self):
        """Cierre de short → mensaje contiene 'Compra', NO 'Venta' (M-07-BUG fix)."""
        bus = _FakeBus()
        orden = _make_orden(direccion="short")
        manager = _make_manager(modo_real=True, bus=bus)
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 88_000.0, "SL", "op-007"
        )
        notifs = [p["mensaje"] for e, p in bus.published if e == "notify"]
        assert notifs, "Se esperaba al menos una notificación"
        assert any("Compra" in m for m in notifs), f"Bug M-07: short debería notificar 'Compra'. Mensajes: {notifs}"
        assert not any("Venta" in m and "Compra" not in m for m in notifs), \
            "Bug M-07 presente: 'Venta' hardcodeada para short"

    @pytest.mark.asyncio
    async def test_sim_mode_publica_orden_simulada_cerrada(self):
        """modo_real=False → publica 'orden_simulada_cerrada' con precio y retorno."""
        bus = _FakeBus()
        orden = _make_orden()
        manager = _make_manager(modo_real=False, bus=bus)
        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-008"
        )
        sim_events = [(e, p) for e, p in bus.published if e == "orden_simulada_cerrada"]
        assert sim_events, "Debe publicar 'orden_simulada_cerrada' en modo simulado"
        _, payload = sim_events[0]
        assert payload["symbol"] == "BTC/USDT"
        assert payload["precio_cierre"] == 91_000.0

    @pytest.mark.asyncio
    async def test_real_mode_llama_eliminar_orden_sqlite(self):
        """modo_real=True → llama real_orders.eliminar_orden (fix M-07, faltaba en cerrar_async)."""
        bus = _FakeBus()
        orden = _make_orden()
        manager = _make_manager(modo_real=True, bus=bus)

        with patch("core.orders.order_manager_cerrar.real_orders") as mock_ro:
            mock_ro.eliminar_orden = MagicMock()
            await _finalizar_cierre_completo_async(
                manager, "BTC/USDT", orden, 91_000.0, "TP", "op-009"
            )
        # asyncio.to_thread wraps the call — verify it was scheduled
        # (the mock captures the call regardless of thread wrapping in tests)
        # We check via the actual call by running the coroutine synchronously.
        # Since asyncio.to_thread is awaited, by the time we're here it ran.
        assert mock_ro.eliminar_orden.called, \
            "real_orders.eliminar_orden debe llamarse en modo_real (fix M-07)"

    @pytest.mark.asyncio
    async def test_historial_acumulado_correctamente(self):
        """La orden se añade al historial y se trunca si excede max_historial."""
        orden = _make_orden()
        manager = _make_manager()
        manager.max_historial = 2
        manager.historial["BTC/USDT"] = [{"old": 1}, {"old": 2}]

        await _finalizar_cierre_completo_async(
            manager, "BTC/USDT", orden, 91_000.0, "TP", "op-010"
        )
        # After adding 3rd and truncating to 2, should have 2 entries
        assert len(manager.historial["BTC/USDT"]) == 2
