"""Resolución de claves JSON por símbolo (exacta vs mismo activo / distinto quote).

Evita que ``BTC/USDT`` quede sin rama útil cuando el JSON solo define ``BTC/EUR``,
sin conversión FX: se reutiliza la configuración del mismo **base** cuando hay
exactamente una entrada candidata.
"""
from __future__ import annotations

from typing import Any, Mapping, TypeVar

T = TypeVar("T")


def resolve_symbol_branch(symbol: str, data: Mapping[str, T]) -> tuple[str | None, T | None, str]:
    """Busca ``symbol`` en ``data`` (claves string).

    Returns
    -------
    (matched_key, value, resolution)
        ``resolution`` ∈ ``exact`` | ``base_alias`` | ``ambiguous`` | ``missing``.
    """

    if not isinstance(data, Mapping) or not data:
        return None, None, "missing"

    sym_u = str(symbol).strip().upper()
    if not sym_u:
        return None, None, "missing"

    by_upper: dict[str, str] = {}
    for raw_key in data.keys():
        if not isinstance(raw_key, str):
            continue
        ku = raw_key.strip().upper()
        if not ku:
            continue
        by_upper.setdefault(ku, raw_key)

    if sym_u in by_upper:
        orig = by_upper[sym_u]
        return orig, data.get(orig), "exact"

    if "/" not in sym_u:
        return None, None, "missing"

    base, _quote = sym_u.split("/", 1)
    base = base.strip().upper()
    if not base:
        return None, None, "missing"

    candidates: list[str] = []
    for ku, orig in by_upper.items():
        if ku == "DEFAULT":
            continue
        if not ku.startswith(base + "/"):
            continue
        if ku == sym_u:
            continue
        val = data.get(orig)
        if isinstance(val, dict):
            candidates.append(orig)

    if len(candidates) == 1:
        o = candidates[0]
        return o, data.get(o), "base_alias"
    if len(candidates) > 1:
        return None, None, "ambiguous"
    return None, None, "missing"
