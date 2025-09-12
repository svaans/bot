import json
import os
from ast import literal_eval
from collections import defaultdict
from dataclasses import dataclass, field
from math import isclose
from typing import Dict, Iterable, Optional

import pandas as pd
from core.utils.utils import configurar_logger
log = configurar_logger('pesos')


def normalizar_pesos(
    pesos_actuales: Dict[str, float],
    total: float = 100,
    peso_min: float = 0.5,
    factor_temporal: Optional[float] = None,
) -> Dict[str, float]:
    """Normaliza los pesos respetando un mínimo final mediante pegging iterativo."""
    if factor_temporal:
        pesos_actuales = {k: v * factor_temporal for k, v in pesos_actuales.items()}

    restantes = dict(pesos_actuales)
    asignados: Dict[str, float] = {}
    total_restante = total

    while restantes:
        suma = sum(restantes.values())
        if isclose(suma, 0.0, rel_tol=1e-12, abs_tol=1e-12):
            for k in restantes:
                asignados[k] = peso_min
            break
        factor = total_restante / suma
        escalados = {k: v * factor for k, v in restantes.items()}
        menores = {k: v for k, v in escalados.items() if v < peso_min}
        if not menores:
            asignados.update(escalados)
            break
        for k in menores:
            asignados[k] = peso_min
            del restantes[k]
            total_restante -= peso_min
            if total_restante <= 0:
                for k2 in restantes:
                    asignados[k2] = peso_min
                restantes.clear()
                break

    return {k: round(v, 4) for k, v in asignados.items()}


@dataclass
class GestorPesos:
    """Maneja la carga, almacenamiento y cálculo de pesos por estrategia."""
    ruta: str = 'config/estrategias_pesos.json'
    pesos: Dict[str, Dict[str, float]] = field(init=False, default_factory=dict
        )

    def __post_init__(self) ->None:
        self.pesos = self._cargar_pesos()

    def _cargar_pesos(self) ->Dict[str, Dict[str, float]]:
        """Carga pesos desde JSON validando el formato y repara si es necesario."""
        if not os.path.exists(self.ruta):
            log.error(f'❌ No se encontró archivo de pesos: {self.ruta}')
            raise ValueError('Archivo de pesos inexistente')
        try:
            with open(self.ruta, 'r', encoding='utf-8') as f:
                datos = json.load(f)
        except json.JSONDecodeError as e:
            log.error(f'❌ Error cargando pesos desde JSON: {e}')
            datos = self._restaurar_desde_base()
        if not isinstance(datos, dict) or not datos:
            raise ValueError('❌ Archivo de pesos inválido o vacío.')
        return datos

    def _restaurar_desde_base(self) ->dict:
        """Restaura pesos desde una copia base."""
        ruta_base = 'config/estrategias_pesos_base.json'
        if not os.path.exists(ruta_base):
            raise ValueError('No hay copia base para recuperar pesos')
        with open(ruta_base, 'r', encoding='utf-8') as base:
            datos = json.load(base)
        with open(self.ruta, 'w', encoding='utf-8') as reparado:
            json.dump(datos, reparado, indent=4)
        log.info('🔄 Pesos restaurados desde copia base')
        return datos

    def guardar(self, pesos: Dict[str, Dict[str, float]]) ->None:
        """Guarda los pesos actualizados en disco."""
        try:
            with open(self.ruta, 'w', encoding='utf-8') as f:
                json.dump(pesos, f, indent=4)
            self.pesos = pesos
            log.info('✅ Pesos guardados.')
        except Exception as e:
            log.error(f'❌ Error al guardar pesos: {e}')

    def obtener_peso(self, estrategia: str, symbol: str) ->float:
        return self.pesos.get(symbol, {}).get(estrategia, 0.0)

    def obtener_pesos_symbol(self, symbol: str) ->Dict[str, float]:
        return self.pesos.get(symbol, {})

    def calcular_desde_backtest(
        self,
        simbolos: Iterable[str] | list[str],
        carpeta: str = 'backtesting',
        escala: int = 20,
    ) -> None:
        """Recalcula pesos desde los CSV de órdenes ganadoras por símbolo."""
        pesos_por_symbol = {}
        for symbol in simbolos:
            ruta = (
                f"{carpeta}/ordenes_{symbol.replace('/', '_')}_resultado.csv")
            if not os.path.exists(ruta):
                log.warning(f'❌ Archivo no encontrado: {ruta}')
                continue
            try:
                df = pd.read_csv(ruta, encoding='utf-8')
            except pd.errors.EmptyDataError:
                log.warning(f'⚠️ Archivo vacío: {ruta}')
                continue
            conteo = defaultdict(int)
            for _, fila in df.iterrows():
                if fila.get('resultado') != 'ganancia':
                    continue
                try:
                    estrategias = literal_eval(fila['estrategias_activas'])
                    for estrategia, activa in estrategias.items():
                        if activa:
                            conteo[estrategia] += 1
                except Exception as e:
                    log.warning(f'⚠️ Estrategia mal formateada: {e}')
                    continue
            total = sum(conteo.values())
            if total == 0:
                continue
            normalizados = {k: round(v / total * 10, 2) for k, v in conteo.
                items()}
            suma_actual = sum(normalizados.values())
            factor = escala / suma_actual if suma_actual > 0 else 1.0
            reescalados = {k: round(v * factor, 2) for k, v in normalizados
                .items()}
            pesos_por_symbol[symbol] = reescalados
            log.info(f'📊 {symbol}: {reescalados}')
        self.guardar(pesos_por_symbol)


@dataclass
class GestorPesosSalidas:
    """Maneja la carga de pesos para estrategias de salida."""
    ruta: str = 'config/pesos_salidas.json'
    pesos: Dict[str, Dict[str, float]] = field(init=False, default_factory=dict
        )

    def __post_init__(self) ->None:
        self.pesos = self._cargar_pesos()

    def _cargar_pesos(self) ->Dict[str, Dict[str, float]]:
        if not os.path.exists(self.ruta):
            log.error(f'❌ No se encontró archivo de pesos: {self.ruta}')
            raise ValueError('Archivo de pesos inexistente')
        try:
            with open(self.ruta, 'r', encoding='utf-8') as fh:
                datos = json.load(fh)
        except json.JSONDecodeError as e:
            log.error(f'❌ Error cargando pesos de salida: {e}')
            datos = {}
        if not isinstance(datos, dict):
            raise ValueError('❌ Archivo de pesos de salida inválido')
        return datos

    def obtener(self, razon: str, symbol: str) ->float:
        return self.pesos.get(symbol, {}).get(razon, 0.0)


try:
    gestor_pesos = GestorPesos()
except ValueError as e:
    log.warning(f'Inicializando GestorPesos vacío: {e}')
    gestor_pesos = GestorPesos.__new__(GestorPesos)
    gestor_pesos.ruta = 'config/estrategias_pesos.json'
    gestor_pesos.pesos = {}

try:
    gestor_pesos_salidas = GestorPesosSalidas()
except ValueError as e:
    log.warning(f'Inicializando GestorPesosSalidas vacío: {e}')
    gestor_pesos_salidas = GestorPesosSalidas.__new__(GestorPesosSalidas)
    gestor_pesos_salidas.ruta = 'config/pesos_salidas.json'
    gestor_pesos_salidas.pesos = {}


def cargar_pesos_estrategias() -> Dict[str, Dict[str, float]]:
    return gestor_pesos.pesos


def obtener_peso_salida(razon: str, symbol: str) ->float:
    """Devuelve el peso de una estrategia de salida para el símbolo dado."""
    try:
        return gestor_pesos_salidas.obtener(razon, symbol)
    except Exception:
        return 0.0
