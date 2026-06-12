"""Lógica del aprendizaje continuo: estabilidad de escala y dirección del ajuste.

Regresiones cubiertas (bugs de escala detectados en auditoría):
1. El entrenador mezclaba pesos (escala total=100) con scores normalizados
   (0-1) sin renormalizar → todos los pesos decaían ~2-5%/ciclo y el
   puntaje_total (suma de pesos crudos) acababa por debajo del umbral de
   entrada (~1.5-3): el bot dejaría de abrir posiciones.
2. La recalibración semanal persistía softmax÷max (0-1) y además REEMPLAZABA
   el mapa completo, borrando los pesos de símbolos sin órdenes esa semana.

También verifica la propiedad de rentabilidad del mecanismo: tras varios
ciclos, la asignación de peso se desplaza hacia las estrategias con retorno
positivo (proxy de mejora de la esperanza de retorno ponderada).
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from core.strategies.pesos import gestor_pesos
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from learning.registro_aprendizaje import registrar_cierre_para_aprendizaje

SYMBOL = "BTC/USDT"


def _sembrar_historial(n_por_estrategia: int = 12) -> None:
    """Historial determinista: 'ganadora' +5% por op, 'perdedora' -4% por op."""

    base_ts = 1_780_000_000.0
    for i in range(n_por_estrategia):
        registrar_cierre_para_aprendizaje(
            {
                "symbol": SYMBOL,
                "estrategias_activas": json.dumps({"ganadora": True}),
                "retorno_total": 0.05,
                "fecha_cierre": None,
                "timestamp": base_ts + i * 1200,
            }
        )
        registrar_cierre_para_aprendizaje(
            {
                "symbol": SYMBOL,
                "estrategias_activas": json.dumps({"perdedora": True}),
                "retorno_total": -0.04,
                "fecha_cierre": None,
                "timestamp": base_ts + i * 1200 + 600,
            }
        )


def _share(pesos: dict[str, float], estrategia: str) -> float:
    total = sum(pesos.values())
    return pesos[estrategia] / total if total else 0.0


def test_entrenador_mantiene_escala_total() -> None:
    gestor_pesos.pesos[SYMBOL] = {"ganadora": 50.0, "perdedora": 50.0}
    _sembrar_historial()

    for _ in range(5):
        actualizar_pesos_estrategias_symbol(SYMBOL)
        pesos = gestor_pesos.pesos[SYMBOL]
        # Regresión bug de escala: la suma debe mantenerse en total=100 tras
        # cada ciclo (antes decaía ~2-5% por ciclo hacia la escala 0-1).
        assert sum(pesos.values()) == pytest.approx(gestor_pesos.total, abs=1e-6)
        assert all(p >= 0 for p in pesos.values())


def test_entrenador_desplaza_peso_hacia_la_ganadora() -> None:
    gestor_pesos.pesos[SYMBOL] = {"ganadora": 50.0, "perdedora": 50.0}
    _sembrar_historial()

    share_inicial = _share(gestor_pesos.pesos[SYMBOL], "ganadora")
    esperanza_inicial = (
        _share(gestor_pesos.pesos[SYMBOL], "ganadora") * 0.05
        + _share(gestor_pesos.pesos[SYMBOL], "perdedora") * -0.04
    )

    for _ in range(10):
        actualizar_pesos_estrategias_symbol(SYMBOL)

    pesos = gestor_pesos.pesos[SYMBOL]
    share_final = _share(pesos, "ganadora")
    assert share_final > share_inicial, (
        "el aprendizaje debe aumentar la cuota de la estrategia rentable"
    )

    # Proxy de rentabilidad: esperanza de retorno ponderada por peso.
    esperanza_final = _share(pesos, "ganadora") * 0.05 + _share(pesos, "perdedora") * -0.04
    assert esperanza_final > esperanza_inicial


def test_entrenador_persiste_en_la_ruta_del_gestor() -> None:
    gestor_pesos.pesos[SYMBOL] = {"ganadora": 50.0, "perdedora": 50.0}
    _sembrar_historial()
    actualizar_pesos_estrategias_symbol(SYMBOL)
    assert gestor_pesos.ruta.exists()
    data = json.loads(gestor_pesos.ruta.read_text(encoding="utf-8"))
    assert sum(data[SYMBOL].values()) == pytest.approx(gestor_pesos.total, abs=1e-6)


def test_recalibrar_normaliza_escala_y_preserva_otros_simbolos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import learning.recalibrar_semana as rs

    gestor_pesos.pesos.clear()
    gestor_pesos.pesos[SYMBOL] = {"ganadora": 60.0, "perdedora": 40.0}
    gestor_pesos.pesos["ETH/USDT"] = {"x": 70.0, "y": 30.0}
    _sembrar_historial()

    rs.recalibrar_pesos_semana()

    pesos_btc = gestor_pesos.pesos[SYMBOL]
    # Regresión: la recalibración escribía softmax÷max (0-1); debe quedar en
    # la escala del gestor.
    assert sum(pesos_btc.values()) == pytest.approx(gestor_pesos.total, abs=1e-6)
    # Regresión: guardar(snapshot) reemplazaba el mapa entero, borrando los
    # símbolos sin órdenes de la semana.
    assert gestor_pesos.pesos["ETH/USDT"] == {"x": 70.0, "y": 30.0}


def test_ejecutar_ciclo_lee_lo_que_escribe_el_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ejecutar_ciclo debe descubrir los parquet del writer (misma ruta raíz)."""

    import learning.aprendizaje_continuo as ac

    _sembrar_historial(n_por_estrategia=3)
    procesados: list[tuple[str, str]] = []
    monkeypatch.setattr(
        ac, "procesar_simbolo", lambda symbol, ruta: procesados.append((symbol, ruta))
    )
    ac.ejecutar_ciclo()
    assert procesados, "el ciclo no encontró los parquet escritos por el runtime"
    assert procesados[0][0] == SYMBOL


def test_pesos_distintos_producen_pesos_validos_con_historial_mixto() -> None:
    """Caso mixto: ambas estrategias activas en la misma operación."""

    gestor_pesos.pesos[SYMBOL] = {"ganadora": 50.0, "perdedora": 50.0}
    base_ts = 1_780_000_000.0
    for i in range(14):
        registrar_cierre_para_aprendizaje(
            {
                "symbol": SYMBOL,
                "estrategias_activas": json.dumps({"ganadora": True, "perdedora": True}),
                "retorno_total": 0.02 if i % 2 == 0 else -0.01,
                "timestamp": base_ts + i * 900,
            }
        )
    actualizar_pesos_estrategias_symbol(SYMBOL)
    pesos = gestor_pesos.pesos[SYMBOL]
    assert sum(pesos.values()) == pytest.approx(gestor_pesos.total, abs=1e-6)
    assert all(p >= 0 for p in pesos.values())
