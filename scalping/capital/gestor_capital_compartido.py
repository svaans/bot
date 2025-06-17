"""Gestor de capital compartido entre modos."""

from __future__ import annotations

from typing import Dict


class GestorCapitalCompartido:
    def __init__(self, capital_total: float) -> None:
        self.capital_total = capital_total
        self.capital_reservado: Dict[str, float] = {}

    def reservar(self, modo: str, cantidad: float) -> bool:
        disponible = self.capital_total - sum(self.capital_reservado.values())
        if cantidad > disponible:
            return False
        self.capital_reservado[modo] = self.capital_reservado.get(modo, 0) + cantidad
        return True

    def liberar(self, modo: str, cantidad: float) -> None:
        actual = self.capital_reservado.get(modo, 0)
        self.capital_reservado[modo] = max(0.0, actual - cantidad)