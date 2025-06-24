from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class PersistenciaTecnica:
    """Conteo de activaciones consecutivas por estrategia y símbolo."""

    minimo: int = 1
    peso_extra: float = 0.5
    conteo: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def actualizar(self, symbol: str, estrategias: Dict[str, bool]) -> None:
        """Actualiza los contadores de persistencia."""
        actual = self.conteo.setdefault(symbol, {})
        # Reinicia contadores de estrategias que no se evaluaron en esta vela
        for nombre in list(actual.keys()):
            if nombre not in estrategias:
                actual[nombre] = 0

        for nombre, activa in estrategias.items():
            actual[nombre] = actual.get(nombre, 0) + 1 if activa else 0

    def es_persistente(self, symbol: str, estrategia: str) -> bool:
        """Devuelve ``True`` si ``estrategia`` ha estado activa ``minimo`` velas."""
        return self.conteo.get(symbol, {}).get(estrategia, 0) >= self.minimo

    def filtrar_persistentes(self, symbol: str, estrategias: Dict[str, bool]) -> Dict[str, bool]:
        """Actualiza contadores y retorna solo estrategias persistentes."""
        self.actualizar(symbol, estrategias)
        return {e: True for e, act in estrategias.items() if act and self.es_persistente(symbol, e)}


def coincidencia_parcial(buffer: List[dict], pesos: Dict[str, float], ventanas: int = 5) -> float:
    """Calcula un puntaje de coincidencia parcial de estrategias en las últimas ``ventanas`` velas."""
    if len(buffer) < ventanas:
        return 0.0

    conteo: Dict[str, int] = {}
    for vela in buffer[-ventanas:]:
        for nombre, activa in vela.get("estrategias_activas", {}).items():
            if activa:
                conteo[nombre] = conteo.get(nombre, 0) + 1

    puntaje = 0.0
    for nombre, veces in conteo.items():
        fraccion = veces / ventanas
        puntaje += pesos.get(nombre, 0) * fraccion

    return puntaje
