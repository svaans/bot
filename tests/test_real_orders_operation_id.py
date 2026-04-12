"""Extracción de operation_id desde respuestas estilo CCXT."""

from __future__ import annotations

from core.orders.order_helpers import extract_ccxt_operation_id


def test_operation_id_prefiere_client_order_id() -> None:
    o = {"clientOrderId": "my-cid", "id": "999"}
    assert extract_ccxt_operation_id(o) == "my-cid"


def test_operation_id_desde_info_anidado() -> None:
    o = {"info": {"clientOrderId": "nested-cid"}}
    assert extract_ccxt_operation_id(o) == "nested-cid"


def test_operation_id_vacio_devuelve_none() -> None:
    assert extract_ccxt_operation_id({}) is None
