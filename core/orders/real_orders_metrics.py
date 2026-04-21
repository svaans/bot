# core/orders/real_orders_metrics.py — acumulación de fee/pnl por operation_id
from __future__ import annotations

METRICAS_OPERACION: dict[str, dict[str, float]] = {}


def acumular_metricas(operation_id: str, response: dict) -> dict[str, float]:
    """Acumula comisiones y PnL por ``operation_id`` a partir de la respuesta.

    Para cada trade se calcula el PnL con el signo correcto según ``side`` y se
    deduce la comisión de forma proporcional a la base correspondiente
    (notional o cantidad) dependiendo de la moneda en la que se cobra la fee.
    """

    metricas = METRICAS_OPERACION.setdefault(
        operation_id, {"fee": 0.0, "pnl": 0.0}
    )

    symbol = (
        response.get("symbol")
        or response.get("info", {}).get("symbol")
        or ""
    )
    base, quote = (symbol.split("/") if "/" in symbol else ("", ""))

    trades = response.get("trades") or []
    for trade in trades:
        side = trade.get("side") or ""
        price = float(trade.get("price") or 0.0)
        amount = float(trade.get("amount") or 0.0)
        cost = float(trade.get("cost") or price * amount)

        # Fees
        fee_info = trade.get("fee") or {}
        fee_cost = fee_info.get("cost")
        currency = fee_info.get("currency")
        if fee_cost is None:
            rate = fee_info.get("rate")
            if rate is not None:
                if currency == base:
                    fee_cost = float(rate) * amount
                elif currency == quote:
                    fee_cost = float(rate) * cost
                else:
                    fee_cost = 0.0
        elif currency not in (base, quote):
            fee_cost = 0.0
        fee_cost = float(fee_cost or 0.0)
        metricas["fee"] += fee_cost

        # PnL sign: buys are negative cash flow, sells positive
        pnl = cost if side == "sell" else -cost
        pnl -= fee_cost
        metricas["pnl"] += pnl

    # Some responses include aggregated fees outside of ``trades``
    for fee in response.get("fees", []) or []:
        try:
            currency = fee.get("currency")
            if currency not in (base, quote):
                continue
            costo = float(fee.get("cost") or 0.0)
        except Exception:
            continue
        metricas["fee"] += costo
        metricas["pnl"] -= costo

    return metricas
