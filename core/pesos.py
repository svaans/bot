import json
import os
from dataclasses import dataclass, field
from typing import Dict

import pandas as pd
from collections import defaultdict
from core.logger import configurar_logger

log = configurar_logger("pesos")

@dataclass
class GestorPesos:
    """Maneja la carga y acceso a los pesos de estrategias."""

    ruta: str = "config/estrategias_pesos.json"
    pesos: Dict[str, Dict[str, float]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.pesos = self._cargar_pesos()

    def _cargar_pesos(self) -> Dict[str, Dict[str, float]]:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError as e:
                    log.error(f"❌ Error cargando pesos desde JSON: {e}")
                    raise
        else:
            log.warning(f"❌ No se encontró archivo de pesos: {self.ruta}")
        return {}

    def guardar(self, pesos: Dict[str, Dict[str, float]]) -> None:
        self.pesos = pesos
        with open(self.ruta, "w") as f:
            json.dump(self.pesos, f, indent=4)
        log.info("✅ Pesos guardados.")

    def obtener_peso(self, estrategia: str, symbol: str) -> float:
        return self.pesos.get(symbol, {}).get(estrategia, 0.0)

    def obtener_pesos_symbol(self, symbol: str) -> Dict[str, float]:
        return self.pesos.get(symbol, {})

    def calcular_desde_backtest(self, simbolos, carpeta="backtesting", escala=20) -> None:
        pesos_por_symbol = {}
        for symbol in simbolos:
            ruta = f"{carpeta}/ordenes_{symbol.replace('/', '_')}_resultado.csv"
            if not os.path.exists(ruta):
                log.warning(f"❌ Archivo no encontrado: {ruta}")
                continue
            try:
                df = pd.read_csv(ruta)
            except pd.errors.EmptyDataError:
                log.warning(f"⚠️ Archivo vacío: {ruta}")
                continue

            conteo = defaultdict(int)
            for _, fila in df.iterrows():
                if fila.get("resultado") != "ganancia":
                    continue
                try:
                    estrategias = json.loads(fila["estrategias_activas"].replace("'", "\"") )
                except json.JSONDecodeError:
                    log.warning(f"❌ JSON inválido en fila: {fila['estrategias_activas']}")
                    continue

                for estrategia, activa in estrategias.items():
                    if activa:
                        conteo[estrategia] += 1

            total = sum(conteo.values())
            if total == 0:
                continue

            normalizados = {k: round(v / total * 10, 2) for k, v in conteo.items()}
            suma_actual = sum(normalizados.values())
            factor = escala / suma_actual if suma_actual > 0 else 1
            reescalados = {k: round(v * factor, 2) for k, v in normalizados.items()}

            pesos_por_symbol[symbol] = reescalados
            log.info(f"📊 {symbol}: {reescalados}")

        self.guardar(pesos_por_symbol)

gestor_pesos = GestorPesos()

def cargar_pesos_estrategias() -> Dict[str, Dict[str, float]]:
    """Carga y devuelve los pesos actuales de las estrategias."""
    return GestorPesos().pesos
