# core/strategies/pesos.py
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Union

# Rutas configurables por entorno (con valores por defecto)
ESTRATEGIAS_PESOS_PATH = Path(os.getenv("ESTRATEGIAS_PESOS_PATH", "estado/pesos_estrategias.json"))
PESOS_SALIDAS_PATH = Path(os.getenv("PESOS_SALIDAS_PATH", "estado/pesos_salidas.json"))

Number = Union[int, float]


# ----------------------------- Utilidades IO ----------------------------- #

def _read_json(path: Path) -> dict:
    """Lee JSON y devuelve dict. Si no existe o está corrupto, devuelve {}."""
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _atomic_write_json(path: Path, data: dict) -> None:
    """Escribe JSON de forma atómica (tmp + replace). Crea directorio si no existe."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# ----------------------------- Normalización ---------------------------- #

def _normalize_with_floor(raw: Dict[str, Number], *, total: float = 100.0, piso: float = 1.0) -> Dict[str, float]:
    """Normaliza `raw` a que la suma sea `total` y cada clave tenga al menos `piso`.

    Algoritmo:
      1) Limpia negativos/Nan → 0.0. Si todo queda 0, reparte igual.
      2) Piso efectivo = min(piso, total / n_claves) para no superar `total`.
      3) Asigna piso a todas las claves.
      4) Distribuye el resto proporcionalmente a los pesos originales (o uniforme si sum=0).
      5) Ajuste final al mayor peso para garantizar suma exacta==`total` y evitar error de redondeo.
    """
    if total <= 0:
        return {k: 0.0 for k in raw}

    keys = list(raw.keys())
    n = len(keys)
    if n == 0:
        return {}

    clean = {k: max(0.0, _safe_float(raw.get(k, 0.0))) for k in keys}
    sum_clean = sum(clean.values())

    # Si todo 0 → repartir uniforme
    base_props = {k: (1.0 / n) for k in keys} if sum_clean == 0 else {k: clean[k] / sum_clean for k in keys}

    piso_efectivo = min(max(piso, 0.0), total / n)

    restante = max(0.0, total - piso_efectivo * n)
    out = {k: piso_efectivo + restante * base_props[k] for k in keys}

    # Ajuste final por errores de redondeo
    diff = total - sum(out.values())
    if abs(diff) > 1e-9:
        # Sumar al mayor peso (o restar si diff<0)
        kmax = max(out, key=lambda k: out[k])
        out[kmax] = max(0.0, out[kmax] + diff)

    return out


# ----------------------------- Gestor de pesos (entradas) ----------------------------- #

@dataclass
class GestorPesos:
    """Gestiona pesos por símbolo → {estrategia: peso} para el scoring de ENTRADAS."""
    ruta: Path
    total: float = 100.0
    piso: float = 1.0
    pesos: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # ---- construcción / persistencia ----
    @classmethod
    def from_file(cls, ruta: Path, *, total: float = 100.0, piso: float = 1.0) -> "GestorPesos":
        data = _read_json(ruta)
        # data esperado: { "BTC/USDT": {"rsi": 15.2, "tendencia": 30.0, ...}, ... }
        pesos: Dict[str, Dict[str, float]] = {}
        for symbol, d in data.items():
            if isinstance(d, dict):
                pesos[symbol] = {k: _safe_float(v, 0.0) for k, v in d.items()}
        return cls(ruta=ruta, total=total, piso=piso, pesos=pesos)

    def guardar(self) -> None:
        _atomic_write_json(self.ruta, self.pesos)

    # ---- API principal ----
    def obtener_pesos_symbol(self, symbol: str) -> Dict[str, float]:
        """Devuelve dict de pesos para ``symbol`` (vacío si no hay)."""
        return dict(self.pesos.get(symbol.upper(), {}))

    def obtener_peso(self, estrategia: str, symbol: str) -> float:
        """Obtiene el peso asignado a ``estrategia`` para ``symbol``."""

        if not estrategia:
            return 0.0
        bucket = self.pesos.get(symbol.upper(), {})
        return float(bucket.get(estrategia, 0.0))

    def set_pesos_symbol(self, symbol: str, pesos_crudos: Dict[str, Number]) -> Dict[str, float]:
        """Establece pesos para `symbol` normalizando a (total, piso). Devuelve los pesos normalizados."""
        symbol = symbol.upper()
        normalizados = _normalize_with_floor(pesos_crudos, total=self.total, piso=self.piso)
        self.pesos[symbol] = normalizados
        self.guardar()
        return dict(normalizados)

    def merge_add(self, symbol: str, delta: Dict[str, Number]) -> Dict[str, float]:
        """Suma (no normalizado) valores a los pesos actuales y renormaliza."""
        base = self.pesos.get(symbol.upper(), {})
        acumulado = {k: _safe_float(base.get(k, 0.0)) + _safe_float(delta.get(k, 0.0)) for k in set(base) | set(delta)}
        return self.set_pesos_symbol(symbol, acumulado)

    # ---- cálculo desde backtest ----
    def calcular_desde_backtest(
        self,
        rutas_csv: Iterable[Union[str, Path]],
        *,
        campo_estrategias: str = "estrategias_activas",
        campo_retorno: str = "retorno_total",
        filtro_symbol: str | None = None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Deriva pesos por símbolo analizando CSVs de resultados de backtest.

        Heurística:
          - Suma contribuciones positivas de retorno de cada estrategia.
          - Si `estrategias_activas` es dict, usa las claves; si es lista, cuenta 1 por estrategia.
          - Si `retorno_total` <= 0, no suma (opcionalmente podrías penalizar).

        Devuelve:
          { "BTC/USDT": {"tendencia": 40.0, "rsi": 10.0, ...}, ... } normalizado a `total`.
        """
        import csv
        from ast import literal_eval

        acumulado: Dict[str, Dict[str, float]] = {}  # symbol -> estrategia -> score acumulado

        for ruta in rutas_csv:
            p = Path(ruta)
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = (row.get("symbol") or row.get("par") or "").upper()
                    if not symbol:
                        continue
                    if filtro_symbol and symbol != filtro_symbol.upper():
                        continue

                    ret = _safe_float(row.get(campo_retorno), 0.0)
                    if ret <= 0:
                        continue  # no sumamos pérdidas

                    raw = row.get(campo_estrategias, "")
                    estrategias: List[Tuple[str, float]] = []
                    try:
                        # Puede venir como JSON o como literal de Python
                        parsed = json.loads(raw)
                    except Exception:
                        try:
                            parsed = literal_eval(raw)
                        except Exception:
                            parsed = {}

                    if isinstance(parsed, dict):
                        # { "rsi": 1, "tendencia": 1.5, ... }  (los valores se ignoran o pueden ponderar)
                        for k, v in parsed.items():
                            estrategias.append((str(k), 1.0 if not isinstance(v, (int, float)) else float(v)))
                    elif isinstance(parsed, (list, tuple)):
                        for k in parsed:
                            estrategias.append((str(k), 1.0))
                    elif isinstance(parsed, str) and parsed:
                        # Texto simple separado por comas
                        for k in parsed.split(","):
                            k = k.strip()
                            if k:
                                estrategias.append((k, 1.0))
                    else:
                        continue

                    bucket = acumulado.setdefault(symbol, {})
                    for nombre, peso_local in estrategias:
                        bucket[nombre] = _safe_float(bucket.get(nombre, 0.0)) + max(0.0, ret) * max(0.0, _safe_float(peso_local, 1.0))

        # Normalizar por símbolo y persistir
        for symbol, d in acumulado.items():
            self.set_pesos_symbol(symbol, d)

        return {s: dict(self.pesos.get(s, {})) for s in acumulado}


# ----------------------------- Gestor de pesos (salidas) ----------------------------- #

@dataclass
class GestorPesosSalidas:
    """Gestiona pesos por estrategia de SALIDA (global, no por símbolo)."""
    ruta: Path
    total: float = 100.0
    piso: float = 5.0
    pesos: Dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_file(cls, ruta: Path, *, total: float = 100.0, piso: float = 5.0) -> "GestorPesosSalidas":
        data = _read_json(ruta)
        pesos = {k: _safe_float(v, 0.0) for k, v in data.items()} if isinstance(data, dict) else {}
        return cls(ruta=ruta, total=total, piso=piso, pesos=pesos)

    def guardar(self) -> None:
        _atomic_write_json(self.ruta, self.pesos)

    def obtener_peso_salida(self, estrategia: str) -> float:
        """Devuelve el peso (0..total) para la estrategia indicada."""
        return float(self.pesos.get(estrategia, 0.0))

    def set_pesos(self, pesos_crudos: Dict[str, Number]) -> Dict[str, float]:
        normalizados = _normalize_with_floor(pesos_crudos, total=self.total, piso=self.piso)
        self.pesos = normalizados
        self.guardar()
        return dict(normalizados)

    def merge_add(self, delta: Dict[str, Number]) -> Dict[str, float]:
        base = self.pesos
        acumulado = {k: _safe_float(base.get(k, 0.0)) + _safe_float(delta.get(k, 0.0)) for k in set(base) | set(delta)}
        return self.set_pesos(acumulado)


# ----------------------------- Singletons prácticos ----------------------------- #

# Carga perezosa/segura para que existan incluso si los archivos faltan
try:
    gestor_pesos: GestorPesos = GestorPesos.from_file(ESTRATEGIAS_PESOS_PATH)
except Exception:
    gestor_pesos = GestorPesos(ruta=ESTRATEGIAS_PESOS_PATH)

try:
    gestor_pesos_salidas: GestorPesosSalidas = GestorPesosSalidas.from_file(PESOS_SALIDAS_PATH)
except Exception:
    gestor_pesos_salidas = GestorPesosSalidas(ruta=PESOS_SALIDAS_PATH)


def cargar_pesos_estrategias(
    *, ruta: str | Path | None = None, total: float | None = None, piso: float | None = None
) -> GestorPesos:
    """Recarga los pesos de estrategias desde disco y devuelve el gestor global."""

    global gestor_pesos
    ruta_path = Path(ruta) if ruta is not None else gestor_pesos.ruta
    gestor_pesos = GestorPesos.from_file(
        ruta_path,
        total=total if total is not None else gestor_pesos.total,
        piso=piso if piso is not None else gestor_pesos.piso,
    )
    return gestor_pesos


def obtener_peso_salida(estrategia: str, _symbol: str | None = None) -> float:
    """Devuelve el peso configurado para una estrategia de salida."""

    if not estrategia:
        return 0.0
    return gestor_pesos_salidas.obtener_peso_salida(estrategia)

