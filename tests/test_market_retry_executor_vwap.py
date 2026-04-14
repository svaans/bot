"""VWAP de fills en reintentos de compra a mercado."""
from __future__ import annotations

from types import SimpleNamespace
import pytest

from core.orders import market_retry_executor as mre


@pytest.mark.asyncio
async def test_ejecutar_market_buy_vwap(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def fake_market(
        symbol: str,
        cantidad: float,
        operation_id: str | None,
        *,
        order_attempt: int = 1,
        precio_senal_bot=None,
    ) -> dict:
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "ejecutado": 1.0,
                "restante": 1.0,
                "status": "PARTIAL",
                "min_qty": 0.001,
                "fee": 0.0,
                "pnl": 0.0,
                "precio_fill": 100.0,
            }
        return {
            "ejecutado": 1.0,
            "restante": 0.0,
            "status": "FILLED",
            "min_qty": 0.001,
            "fee": 0.0,
            "pnl": 0.0,
            "precio_fill": 110.0,
        }

    async def _noop_sleep(*_a: object, **_k: object) -> None:
        return

    monkeypatch.setattr(mre.real_orders, "ejecutar_orden_market", fake_market)
    monkeypatch.setattr(mre.asyncio, "sleep", _noop_sleep)

    log = SimpleNamespace(
        error=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        info=lambda *a, **k: None,
        log=lambda *a, **k: None,
    )
    ex = mre.MarketRetryExecutor(log=log, bus=None)
    out = await ex._ejecutar_market_buy("BTC/USDT", 2.0, "op-1", {})
    assert out.executed == pytest.approx(2.0)
    assert out.precio_fill_promedio == pytest.approx(105.0)
