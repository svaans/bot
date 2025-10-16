"""Auditoría automatizada de los adaptadores dinámicos del bot.

Este módulo genera datasets sintéticos reproducibles para cada símbolo
configurado y compara la configuración base contra la configuración
adaptada que producen ``core.adaptador_configuracion_dinamica`` y
``core.adaptador_dinamico``. También valida el cálculo de umbrales
adaptativos y de niveles TP/SL para detectar inconsistencias de riesgo.

Se puede ejecutar como script para generar un informe en
``estado/auditoria_adaptadores.json`` con las desviaciones encontradas y
recomendaciones por símbolo::

    python -m core.diag.auditoria_adaptadores

"""
from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import numpy as np
import pandas as pd

from core.adaptador_umbral import calcular_umbral_adaptativo
from core.utils.market_utils import calcular_atr_pct, calcular_slope_pct
from indicadores.helpers import get_rsi
from indicadores.retornos_volatilidad import (
    retornos_log,
    retornos_simples,
    verificar_consistencia,
    volatilidad_welford,
)


DEFAULT_OUTPUT = Path("estado/auditoria_adaptadores.json")


@lru_cache
def _adaptador_dinamico():
    """Carga diferida del módulo evitando el ciclo con ``data_feed``."""

    import sys
    import types

    if "data_feed.candle_builder" not in sys.modules:
        dummy = types.ModuleType("data_feed.candle_builder")

        async def _no_backfill(*_args, **_kwargs):  # pragma: no cover - auditoría
            raise RuntimeError("backfill no disponible en modo auditoría")

        dummy.backfill = _no_backfill
        sys.modules["data_feed.candle_builder"] = dummy
    return import_module("core.adaptador_dinamico")


@lru_cache
def _config_manager():
    """Carga diferida del gestor de configuración dinámica."""

    _adaptador_dinamico()
    return import_module("core.config_manager.dinamica")


@dataclass
class EvaluacionAdaptador:
    symbol: str
    volatilidad: float
    slope_pct: float
    atr_pct: float
    rsi: float
    umbral_entrada: float
    tp_sl: Dict[str, Any]
    desviaciones: List[Dict[str, Any]]
    nuevos_parametros: Dict[str, Any]
    recomendaciones: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "volatilidad": self.volatilidad,
            "slope_pct": self.slope_pct,
            "atr_pct": self.atr_pct,
            "rsi": self.rsi,
            "umbral_entrada": self.umbral_entrada,
            "tp_sl": self.tp_sl,
            "desviaciones": self.desviaciones,
            "nuevos_parametros": self.nuevos_parametros,
            "recomendaciones": self.recomendaciones,
        }


def _generar_dataset_sintetico(symbol: str, *, barras: int = 240) -> pd.DataFrame:
    """Crea una serie OHLCV sintética reproducible para ``symbol``.

    Se utiliza un generador pseudoaleatorio determinístico basado en el
    nombre del símbolo para que la auditoría sea estable entre ejecuciones.
    """

    seed = abs(hash(symbol)) % (2**32)
    rng = np.random.default_rng(seed)

    base_price = rng.uniform(15, 250)
    drift = rng.normal(0.0002, 0.0004)
    vol = rng.uniform(0.0006, 0.0035)
    returns = rng.normal(drift, vol, size=barras)

    close = base_price * np.exp(np.cumsum(returns))
    open_ = np.concatenate(([close[0]], close[:-1]))

    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0.0007, vol / 2, size=barras)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0.0007, vol / 2, size=barras)))
    low = np.maximum(low, 0.0001)

    volume_base = rng.uniform(500, 2500)
    volume_noise = rng.normal(0.0, volume_base * 0.1, size=barras)
    volume = np.maximum(volume_base + volume_noise, 1.0)

    timestamps = np.arange(barras, dtype="int64") * 60_000
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
    return df


def _float_delta(a: Any, b: Any) -> float | None:
    try:
        return float(b) - float(a)
    except (TypeError, ValueError):
        return None


def _es_cambio_significativo(base: Any, adaptado: Any, *, tol: float = 1e-4) -> bool:
    if isinstance(base, (int, float)) and isinstance(adaptado, (int, float)):
        return abs(float(base) - float(adaptado)) > tol
    return base != adaptado


def _detectar_desviaciones(
    base_config: Mapping[str, Any],
    config_adaptada: Mapping[str, Any],
    *,
    campos_relevantes: Iterable[str],
) -> List[Dict[str, Any]]:
    desviaciones: List[Dict[str, Any]] = []
    for campo in campos_relevantes:
        base_val = base_config.get(campo)
        adapt_val = config_adaptada.get(campo)
        if adapt_val is None and base_val is None:
            continue
        if not _es_cambio_significativo(base_val, adapt_val):
            continue
        desviacion: Dict[str, Any] = {
            "parametro": campo,
            "base": base_val,
            "adaptado": adapt_val,
        }
        delta = _float_delta(base_val, adapt_val)
        if delta is not None:
            desviacion["delta"] = round(delta, 6)
        desviaciones.append(desviacion)
    return desviaciones


def _armar_tp_sl_resultado(tp_sl_result: Any, precio_actual: float) -> Dict[str, Any]:
    ratio = None
    if tp_sl_result and precio_actual:
        try:
            distancia_sl = precio_actual - float(tp_sl_result.sl)
            distancia_tp = float(tp_sl_result.tp) - precio_actual
            if distancia_sl > 0:
                ratio = distancia_tp / distancia_sl
        except Exception:  # pragma: no cover - defensivo
            ratio = None
    return {
        "sl": getattr(tp_sl_result, "sl", None),
        "tp": getattr(tp_sl_result, "tp", None),
        "atr": getattr(tp_sl_result, "atr", None),
        "multiplicador_sl": getattr(tp_sl_result, "multiplicador_sl", None),
        "multiplicador_tp": getattr(tp_sl_result, "multiplicador_tp", None),
        "ratio_tp_sl": ratio,
        "sl_clamped": getattr(tp_sl_result, "sl_clamped", None),
        "tp_clamped": getattr(tp_sl_result, "tp_clamped", None),
    }


def _generar_recomendaciones(
    *,
    volatilidad: float,
    slope_pct: float,
    atr_pct: float,
    modo_agresivo: bool,
    tp_sl: Mapping[str, Any],
    umbral_entrada: float,
    base_umbral: float,
) -> List[str]:
    recomendaciones: List[str] = []
    if volatilidad > 0.02:
        recomendaciones.append(
            "Volatilidad elevada; incrementar monitoreo y validar límites de exposición diaria."
        )
    if slope_pct < -0.001:
        recomendaciones.append(
            "Pendiente negativa; priorizar estrategias defensivas o confirmar señales antes de entrar."
        )
    if modo_agresivo:
        recomendaciones.append(
            "Modo agresivo activo; revisar límites de riesgo y correlaciones antes de habilitar nuevas posiciones."
        )
    if tp_sl.get("ratio_tp_sl") is not None and tp_sl["ratio_tp_sl"] < 1.5:
        recomendaciones.append(
            "Ratio TP/SL comprimido (<1.5); considerar ampliar objetivos o reducir tamaño de posición."
        )
    if bool(tp_sl.get("sl_clamped")):
        recomendaciones.append(
            "Stop Loss limitado por distancia mínima; revisar tick size o habilitar escalado de salida."
        )
    if umbral_entrada > base_umbral * 1.2:
        recomendaciones.append(
            "Umbral de entrada muy superior al base; validar que las estrategias sigan generando suficientes setups."
        )
    if atr_pct < 0.008:
        recomendaciones.append(
            "ATR% bajo; podría requerirse filtrar señales de baja convicción para evitar sobreoperar."
        )
    return recomendaciones


def auditar_simbolo(symbol: str, base_config: Mapping[str, Any]) -> EvaluacionAdaptador:
    df = _generar_dataset_sintetico(symbol)
    precios = df["close"].tail(200)
    simples = retornos_simples(precios)
    logs = retornos_log(precios)
    volatilidad = 0.0
    if verificar_consistencia(simples, logs) and len(simples) >= 20:
        volatilidad = float(volatilidad_welford(simples.tail(20)))
    slope_pct = float(calcular_slope_pct(precios.tail(60)))
    atr_pct = float(calcular_atr_pct(df))
    rsi_val = get_rsi(df)
    if isinstance(rsi_val, pd.Series):
        rsi_val = float(rsi_val.iloc[-1])
    else:
        rsi_val = float(rsi_val)

    config_adaptada = _config_manager().adaptar_configuracion(symbol, df, dict(base_config))
    umbral_entrada = float(calcular_umbral_adaptativo(symbol, df))
    precio_actual = float(df["close"].iloc[-1])
    tp_sl_result = _adaptador_dinamico().calcular_tp_sl_adaptativos(
        symbol, df, config_adaptada, capital_actual=1_000.0
    )
    tp_sl_data = _armar_tp_sl_resultado(tp_sl_result, precio_actual)

    campos = (
        "factor_umbral",
        "ajuste_volatilidad",
        "riesgo_maximo_diario",
        "ponderar_por_diversidad",
        "modo_agresivo",
        "multiplicador_estrategias_recurrentes",
        "peso_minimo_total",
        "diversidad_minima",
        "cooldown_tras_perdida",
        "sl_ratio",
        "tp_ratio",
    )
    desviaciones = _detectar_desviaciones(base_config, config_adaptada, campos_relevantes=campos)
    nuevos_parametros = {
        clave: config_adaptada[clave]
        for clave in sorted(config_adaptada.keys())
        if clave not in base_config
    }

    recomendaciones = _generar_recomendaciones(
        volatilidad=volatilidad,
        slope_pct=slope_pct,
        atr_pct=atr_pct,
        modo_agresivo=bool(config_adaptada.get("modo_agresivo")),
        tp_sl=tp_sl_data,
        umbral_entrada=umbral_entrada,
        base_umbral=float(base_config.get("factor_umbral", 1.0) * base_config.get("peso_minimo_total", 1.0)),
    )

    return EvaluacionAdaptador(
        symbol=symbol,
        volatilidad=volatilidad,
        slope_pct=slope_pct,
        atr_pct=atr_pct,
        rsi=rsi_val,
        umbral_entrada=umbral_entrada,
        tp_sl=tp_sl_data,
        desviaciones=desviaciones,
        nuevos_parametros=nuevos_parametros,
        recomendaciones=recomendaciones,
    )


def ejecutar_auditoria(*, salida: Path = DEFAULT_OUTPUT) -> Dict[str, Any]:
    configs_optimas = getattr(_adaptador_dinamico(), "CONFIGS_OPTIMAS", {})
    if not configs_optimas:
        raise RuntimeError(
            "No se encontraron configuraciones óptimas. Verifique 'config/configuraciones_optimas.json'."
        )

    evaluaciones = [
        auditar_simbolo(symbol, config)
        for symbol, config in sorted(configs_optimas.items())
    ]

    payload = {
        "symbols_auditados": [ev.symbol for ev in evaluaciones],
        "evaluaciones": [ev.to_dict() for ev in evaluaciones],
    }
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def main() -> None:  # pragma: no cover - punto de entrada manual
    payload = ejecutar_auditoria()
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":  # pragma: no cover - ejecución directa
    main()