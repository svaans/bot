from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Sequence


@dataclass
class PersistenciaTecnica:
    """Conteo de activaciones consecutivas por estrategia y símbolo."""
    minimo: int = 1
    peso_extra: float = 0.5
    conteo: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def actualizar(self, symbol: str, estrategias: Dict[str, bool]) -> None:
        """Actualiza los contadores de persistencia."""
        actual = self.conteo.setdefault(symbol, {})
        for nombre in list(actual.keys()):
            if nombre not in estrategias:
                actual[nombre] = 0
        for nombre, activa in estrategias.items():
            actual[nombre] = actual.get(nombre, 0) + 1 if activa else 0

    def es_persistente(self, symbol: str, estrategia: str) -> bool:
        """Devuelve ``True`` si ``estrategia`` ha estado activa ``minimo`` velas."""
        return self.conteo.get(symbol, {}).get(estrategia, 0) >= self.minimo

    def filtrar_persistentes(
        self,
        symbol: str,
        estrategias: Dict[str, bool],
    ) -> Dict[str, bool]:
        """Actualiza contadores y retorna solo estrategias persistentes."""
        self.actualizar(symbol, estrategias)
        return {
            estrategia: True
            for estrategia, activa in estrategias.items()
            if activa and self.es_persistente(symbol, estrategia)
        }

    def export_state(self) -> dict[str, Any]:
        """Serializa el estado actual para persistirlo en snapshots."""
        conteo = {
            str(symbol): {
                str(estrategia): int(valor)
                for estrategia, valor in estrategias.items()
            }
            for symbol, estrategias in self.conteo.items()
        }
        return {
            'minimo': int(self.minimo),
            'peso_extra': float(self.peso_extra),
            'conteo': conteo,
        }

    def load_state(self, data: Mapping[str, Any]) -> None:
        """Restaura el estado del snapshot previo, ignorando formatos inválidos."""

        if not isinstance(data, Mapping):
            return

        minimo = data.get('minimo')
        if isinstance(minimo, (int, float)):
            self.minimo = int(minimo)

        peso_extra = data.get('peso_extra')
        if isinstance(peso_extra, (int, float)):
            self.peso_extra = float(peso_extra)

        conteo_raw = data.get('conteo')
        if not isinstance(conteo_raw, Mapping):
            return

        conteo: Dict[str, Dict[str, int]] = {}
        for symbol, estrategias in conteo_raw.items():
            if not isinstance(estrategias, Mapping):
                continue
            symbol_key = str(symbol)
            cleaned: Dict[str, int] = {}
            for estrategia, valor in estrategias.items():
                try:
                    cleaned[str(estrategia)] = int(valor)
                except (TypeError, ValueError):
                    continue
            conteo[symbol_key] = cleaned

        self.conteo = conteo


    def coincidencia_parcial(historial: Sequence[dict], pesos: Dict[str, float],
        ventanas: int = 5) -> float:
        """Calcula un puntaje de coincidencia parcial de estrategias en las últimas ``ventanas`` velas."""
        if len(historial) < ventanas:
            return 0.0
        recientes = list(historial)[-ventanas:]
        conteo: Dict[str, int] = {}
        for estrategias in recientes:
            for nombre, activa in estrategias.items():
                if activa:
                    conteo[nombre] = conteo.get(nombre, 0) + 1
        puntaje = 0.0
        for nombre, veces in conteo.items():
            fraccion = veces / ventanas
            puntaje += pesos.get(nombre, 0) * fraccion
        return puntaje
