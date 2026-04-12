"""Comprueba que los nombres en ``core.estrategias`` existen en el loader de entradas.

Ejecución:

    python -m core.diag.auditoria_estrategias

Código de salida distinto de cero si falta alguna estrategia referenciada.

Requiere el paquete ``ta`` (indicadores) para que los módulos de estrategia
se ejecuten al cargar; sin él muchas entradas fallan al importar y el
informe mostrará falsos positivos.
"""

from __future__ import annotations

import sys

from core.estrategias import ESTRATEGIAS_POR_TENDENCIA
from core.strategies.entry.loader import cargar_estrategias


def main() -> int:
    try:
        import ta  # noqa: F401
    except ImportError:
        print(
            "ERROR: instala el paquete `ta` para auditar el registro de estrategias.",
            file=sys.stderr,
        )
        return 2
    registradas = cargar_estrategias()
    esperadas: set[str] = set()
    for _, nombres in ESTRATEGIAS_POR_TENDENCIA.items():
        esperadas.update(nombres)

    faltan = sorted(esperadas - set(registradas.keys()))
    extra = sorted(set(registradas.keys()) - esperadas)

    print(f"Estrategias referenciadas por tendencia: {len(esperadas)}")
    print(f"Estrategias cargadas por el loader: {len(registradas)}")
    if faltan:
        print("Faltan en el loader (no hay módulo/función homónima):")
        for n in faltan:
            print(f"  - {n}")
    if extra:
        print("Módulos de entrada no listados en ESTRATEGIAS_POR_TENDENCIA:")
        for n in extra[:40]:
            print(f"  - {n}")
        if len(extra) > 40:
            print(f"  ... y {len(extra) - 40} más")

    if faltan:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
