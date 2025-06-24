import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional
import pandas as pd
from collections import defaultdict

from core.utils.utils import configurar_logger

log = configurar_logger("pesos")

# -----------------------------------------------------------
# Función utilitaria para normalizar pesos
# -----------------------------------------------------------
def normalizar_pesos(
    pesos_actuales: Dict[str, float],
    total: float = 100,
    peso_min: float = 0.5,
    factor_temporal: Optional[float] = None,
) -> Dict[str, float]:
    """Normaliza los pesos, respetando un mínimo y aplicando factor temporal si se indica."""

    pesos_temporales = {
        estrategia: valor * factor_temporal if factor_temporal else valor
        for estrategia, valor in pesos_actuales.items()
    }

    pesos_min = {
        estrategia: max(valor, peso_min)
        for estrategia, valor in pesos_temporales.items()
    }

    suma_actual = sum(pesos_min.values())
    if suma_actual == 0:
        return {estrategia: 0.0 for estrategia in pesos_min}

    factor = total / suma_actual
    return {
        estrategia: round(valor * factor, 4) for estrategia, valor in pesos_min.items()
    }


# -----------------------------------------------------------
# Clase principal de gestión
# -----------------------------------------------------------
@dataclass
class GestorPesos:
    """Maneja la carga, almacenamiento y cálculo de pesos por estrategia."""
    ruta: str = "config/estrategias_pesos.json"
    pesos: Dict[str, Dict[str, float]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.pesos = self._cargar_pesos()

    def _cargar_pesos(self) -> Dict[str, Dict[str, float]]:
        """Carga pesos desde JSON validando el formato y repara si es necesario."""
        if not os.path.exists(self.ruta):
            log.error(f"❌ No se encontró archivo de pesos: {self.ruta}")
            raise ValueError("Archivo de pesos inexistente")

        try:
            with open(self.ruta, "r") as f:
                datos = json.load(f)
        except json.JSONDecodeError as e:
            log.error(f"❌ Error cargando pesos desde JSON: {e}")
            datos = self._restaurar_desde_base()
        if not isinstance(datos, dict) or not datos:
            raise ValueError("❌ Archivo de pesos inválido o vacío.")
        return datos

    def _restaurar_desde_base(self) -> dict:
        """Restaura pesos desde una copia base."""
        ruta_base = "config/estrategias_pesos_base.json"
        if not os.path.exists(ruta_base):
            raise ValueError("No hay copia base para recuperar pesos")

        with open(ruta_base, "r") as base:
            datos = json.load(base)
        with open(self.ruta, "w") as reparado:
            json.dump(datos, reparado, indent=4)
        log.info("🔄 Pesos restaurados desde copia base")
        return datos

    def guardar(self, pesos: Dict[str, Dict[str, float]]) -> None:
        """Guarda los pesos actualizados en disco."""
        try:
            with open(self.ruta, "w") as f:
                json.dump(pesos, f, indent=4)
            self.pesos = pesos
            log.info("✅ Pesos guardados.")
        except Exception as e:
            log.error(f"❌ Error al guardar pesos: {e}")

    def obtener_peso(self, estrategia: str, symbol: str) -> float:
        return self.pesos.get(symbol, {}).get(estrategia, 0.0)

    def obtener_pesos_symbol(self, symbol: str) -> Dict[str, float]:
        return self.pesos.get(symbol, {})

    def calcular_desde_backtest(
        self, simbolos, carpeta="backtesting", escala=20
    ) -> None:
        """Recalcula pesos desde los CSV de órdenes ganadoras por símbolo."""
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
                    estrategias = json.loads(
                        fila["estrategias_activas"].replace("'", '"')
                    )
                    for estrategia, activa in estrategias.items():
                        if activa:
                            conteo[estrategia] += 1
                except Exception as e:
                    log.warning(f"⚠️ Estrategia mal formateada: {e}")
                    continue

            total = sum(conteo.values())
            if total == 0:
                continue

            normalizados = {
                k: round(v / total * 10, 2) for k, v in conteo.items()
            }

            suma_actual = sum(normalizados.values())
            factor = escala / suma_actual if suma_actual > 0 else 1.0
            reescalados = {
                k: round(v * factor, 2) for k, v in normalizados.items()
            }

            pesos_por_symbol[symbol] = reescalados
            log.info(f"📊 {symbol}: {reescalados}")

        self.guardar(pesos_por_symbol)

# -----------------------------------------------------------
# Gestión de pesos de salida
# -----------------------------------------------------------
@dataclass
class GestorPesosSalidas:
    """Maneja la carga de pesos para estrategias de salida."""
    ruta: str = "config/pesos_salidas.json"
    pesos: Dict[str, Dict[str, float]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.pesos = self._cargar_pesos()

    def _cargar_pesos(self) -> Dict[str, Dict[str, float]]:
        if not os.path.exists(self.ruta):
            log.error(f"❌ No se encontró archivo de pesos: {self.ruta}")
            raise ValueError("Archivo de pesos inexistente")
        try:
            with open(self.ruta, "r") as fh:
                datos = json.load(fh)
        except json.JSONDecodeError as e:
            log.error(f"❌ Error cargando pesos de salida: {e}")
            datos = {}
        if not isinstance(datos, dict):
            raise ValueError("❌ Archivo de pesos de salida inválido")
        return datos

    def obtener(self, razon: str, symbol: str) -> float:
        return self.pesos.get(symbol, {}).get(razon, 0.0)


# -----------------------------------------------------------
# Inicialización automática
# -----------------------------------------------------------
try:
    gestor_pesos = GestorPesos()
    gestor_pesos_salidas = GestorPesosSalidas()
except ValueError as e:
    log.error(e)
    raise


# Función pública para acceder desde otros módulos
def cargar_pesos_estrategias() -> Dict[str, Dict[str, float]]:
    return GestorPesos().pesos

def obtener_peso_salida(razon: str, symbol: str) -> float:
    """Devuelve el peso de una estrategia de salida para el símbolo dado."""
    try:
        return gestor_pesos_salidas.obtener(razon, symbol)
    except Exception:
        return 0.0
