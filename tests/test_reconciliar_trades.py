import types

import pytest

from core.orders import real_orders


@pytest.fixture(autouse=True)
def reset_reconcile_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORDERS_RECONCILE_QTY_TOLERANCE", raising=False)


def _build_stub_client(trades):
    return types.SimpleNamespace(
        load_markets=lambda: {"BTC/USDT": {}},
        fetch_my_trades=lambda symbol, limit=50: trades,
        fetch_ohlcv=lambda *_args, **_kwargs: [],
    )


def test_reconciliar_reports_missing_order(monkeypatch: pytest.MonkeyPatch) -> None:
    trade = {
        "symbol": "BTC/USDT",
        "price": "20000",
        "amount": "0.1",
        "timestamp": 1_700_000_000_000,
        "side": "buy",
    }
    cliente = _build_stub_client([trade])

    monkeypatch.setattr(real_orders, "obtener_cliente", lambda: cliente)
    monkeypatch.setattr(real_orders, "get_symbol_filters", lambda *_args: {"min_qty": 0.0, "min_notional": 0.0})
    monkeypatch.setattr(real_orders, "obtener_orden", lambda _s: None)

    guardar_calls: list[tuple] = []
    monkeypatch.setattr(real_orders, "guardar_orden_real", lambda *a, **k: guardar_calls.append((a, k)))

    divergencias = real_orders.reconciliar_trades_binance(limit=1)

    assert divergencias
    assert divergencias[0]["reason"] == "missing_local_order"
    assert not guardar_calls


def test_reconciliar_invokes_reporter(monkeypatch: pytest.MonkeyPatch) -> None:
    trades = [
        {
            "symbol": "BTC/USDT",
            "price": "20000",
            "amount": "0.1",
            "timestamp": 1_700_000_000_000,
            "side": "buy",
        }
    ]
    cliente = _build_stub_client(trades)

    monkeypatch.setattr(real_orders, "obtener_cliente", lambda: cliente)
    monkeypatch.setattr(real_orders, "get_symbol_filters", lambda *_args: {"min_qty": 0.0, "min_notional": 0.0})
    monkeypatch.setattr(real_orders, "obtener_orden", lambda _s: None)

    reported: list[list[dict]] = []

    divergencias = real_orders.reconciliar_trades_binance(limit=1, reporter=lambda data: reported.append(list(data)))

    assert reported and reported[0] == divergencias
