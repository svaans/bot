"""Utilidades compartidas para normalizar símbolos de Binance."""
from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "SymbolNormalizer",
    "normalize_symbol_for_rest",
    "normalize_symbol_for_ws",
]


@dataclass(frozen=True, slots=True)
class SymbolNormalizer:
    """Normalizadores separados para REST y WebSocket.

    Binance utiliza convenciones distintas entre las APIs REST y WS. Mantener
    funciones desacopladas evita reutilizaciones accidentales entre capas.
    """

    @staticmethod
    def for_rest(symbol: str) -> str:
        """Normaliza símbolos estilo ``BTC/EUR`` a ``BTCEUR``."""

        return symbol.replace("/", "").upper()

    @staticmethod
    def for_ws(symbol: str) -> str:
        """Normaliza símbolos estilo ``BTC/EUR`` a ``btceur``."""

        return symbol.replace("/", "").lower()


def normalize_symbol_for_rest(symbol: str) -> str:
    """Normalización exclusiva para peticiones REST."""

    return SymbolNormalizer.for_rest(symbol)


def normalize_symbol_for_ws(symbol: str) -> str:
    """Normalización exclusiva para streams de WebSocket."""

    return SymbolNormalizer.for_ws(symbol)