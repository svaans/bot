from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass

from core.utils.logger import _JsonFormatter


@dataclass(frozen=True)
class _NestedDummy:
    value: int


class _ObjectWithCustomStr:
    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover - simple helper
        return f"<Dummy value={self.value}>"


def test_json_formatter_sanitizes_non_serializable_payloads() -> None:
    formatter = _JsonFormatter()

    dummy = _ObjectWithCustomStr("abc")
    nested = _NestedDummy(5)

    record = logging.LogRecord(
        name="json-test",
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg="evento %s",
        args=(dummy,),
        exc_info=None,
    )
    record.obj = dummy
    record.nested = {"dummy": dummy, "dataclass": nested}
    record.lista = [dummy, nested]
    record.set = {dummy, nested}
    record.bytes = b"hola"
    record.bytearray = bytearray(b"mundo")

    payload = json.loads(formatter.format(record))

    assert payload["message"].startswith("evento <Dummy value=abc>")
    assert payload["obj"] == "<Dummy value=abc>"
    assert payload["nested"]["dummy"] == "<Dummy value=abc>"
    assert payload["nested"]["dataclass"] == asdict(nested)
    assert payload["lista"][0] == "<Dummy value=abc>"
    assert payload["lista"][1] == asdict(nested)
    assert isinstance(payload["set"], list)
    assert "<Dummy value=abc>" in payload["set"]
    assert asdict(nested) in payload["set"]
    assert payload["bytes"] == "hola"
    assert payload["bytearray"] == "mundo"
    assert record.args == ("<Dummy value=abc>",)
