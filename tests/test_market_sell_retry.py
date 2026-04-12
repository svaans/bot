"""Reintentos de venta a mercado: idempotencia de clientOrderId y status ERROR."""
from __future__ import annotations

from core.orders import real_orders


def test_market_sell_retry_propagates_error_and_client_attempt_offset(monkeypatch):
    attempts: list[int] = []

    def fake_sell(symbol, cantidad, operation_id, *, order_attempt=1):
        attempts.append(order_attempt)
        return {
            "ejecutado": 0.0,
            "restante": cantidad,
            "status": "ERROR",
            "min_qty": 0.0,
            "fee": 0.0,
            "pnl": 0.0,
        }

    monkeypatch.setattr(real_orders, "ejecutar_orden_market_sell", fake_sell)
    out = real_orders._market_sell_retry("BTC/USDT", 1.0, "op-1", order_attempt_start=2)
    assert out["status"] == "ERROR"
    assert out["last_order_attempt"] == 2
    assert attempts == [2]


def test_market_sell_retry_increments_attempt_across_partial_chain(monkeypatch):
    attempts: list[int] = []
    calls = {"n": 0}

    def fake_sell(symbol, cantidad, operation_id, *, order_attempt=1):
        attempts.append(order_attempt)
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "ejecutado": 0.5,
                "restante": 0.5,
                "status": "PARTIAL",
                "min_qty": 0.0001,
                "fee": 0.0,
                "pnl": 0.0,
            }
        return {
            "ejecutado": 0.5,
            "restante": 0.0,
            "status": "FILLED",
            "min_qty": 0.0001,
            "fee": 0.0,
            "pnl": 0.0,
        }

    monkeypatch.setattr(real_orders, "ejecutar_orden_market_sell", fake_sell)
    out = real_orders._market_sell_retry("BTC/USDT", 1.0, "op-1", order_attempt_start=5)
    assert out["status"] == "FILLED"
    assert attempts == [5, 6]
    assert out["last_order_attempt"] == 6
