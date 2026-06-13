"""Tests del puente runtime→aprendizaje (learning/registro_aprendizaje).

Cubre:
1. Escritura/append del parquet por símbolo con timestamp en epoch segundos.
2. Ida y vuelta con ``cargar_historial_operaciones`` (mismo formato).
3. Compatibilidad con ``analizar_estrategias_en_ordenes`` (métricas por estrategia).
4. Hook en ``_finalizar_cierre_completo_async``: cerrar una orden persiste el registro.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

import learning.historial_operaciones as historial_operaciones
from learning.registro_aprendizaje import registrar_cierre_para_aprendizaje


def _registro(symbol: str = "BTC/USDT", retorno: float = 0.05) -> dict:
    # Timestamps relativos a "ahora": analizar_estrategias_en_ordenes filtra por
    # ventana reciente (dias=1), así que fechas fijas se quedan fuera de la
    # ventana según avanza el reloj (time-bomb). Usar now() lo hace robusto.
    ahora = datetime.now(timezone.utc)
    return {
        "symbol": symbol,
        "precio_entrada": 100.0,
        "cantidad": 1.0,
        "estrategias_activas": json.dumps({"rsi": 1.0, "ema": 0.5}),
        "fecha_cierre": ahora.isoformat(),
        "retorno_total": retorno,
        "timestamp": (ahora - timedelta(minutes=5)).isoformat(),
        "detalles_tecnicos": {"slope": 0.1},
    }


def test_escribe_y_appendea_parquet(tmp_path: Path) -> None:
    destino = registrar_cierre_para_aprendizaje(_registro())
    assert destino is not None and destino.exists()
    assert destino.parent == historial_operaciones.CARPETA_ORDENES
    df = pd.read_parquet(destino)
    assert len(df) == 1
    # timestamp en epoch segundos (lo que asumen los consumidores del ciclo)
    assert df["timestamp"].dtype.kind == "f"
    # dicts saneados a JSON para compatibilidad parquet
    assert isinstance(df["detalles_tecnicos"].iloc[0], str)

    registrar_cierre_para_aprendizaje(_registro(retorno=-0.02))
    df = pd.read_parquet(destino)
    assert len(df) == 2


def test_sin_simbolo_no_escribe() -> None:
    assert registrar_cierre_para_aprendizaje({"retorno_total": 1.0}) is None


def test_round_trip_con_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    destino = registrar_cierre_para_aprendizaje(_registro())
    assert destino is not None
    monkeypatch.setattr(historial_operaciones, "MODO_REAL", False)
    hist = historial_operaciones.cargar_historial_operaciones("BTC/USDT")
    assert len(hist.data) >= 1
    assert hist.source == destino


def test_analisis_resultados_consume_el_parquet() -> None:
    from learning.analisis_resultados import analizar_estrategias_en_ordenes

    destino = registrar_cierre_para_aprendizaje(_registro(retorno=0.10))
    assert destino is not None
    metricas = analizar_estrategias_en_ordenes(str(destino), dias=1)
    assert not metricas.empty
    assert set(metricas["estrategia"]) == {"rsi", "ema"}
    assert metricas["retorno_total"].sum() == pytest.approx(0.10)


@pytest.mark.asyncio
async def test_finalizar_cierre_persiste_para_aprendizaje() -> None:
    from core.orders.order_manager_cerrar import _finalizar_cierre_completo_async
    from core.orders.order_model import Order

    orden = Order(
        symbol="ETH/USDT",
        precio_entrada=2000.0,
        cantidad=1.0,
        stop_loss=1900.0,
        take_profit=2200.0,
        timestamp="2026-06-12T08:00:00+00:00",
        estrategias_activas={"rsi": 1.0},
        tendencia="alcista",
        max_price=2100.0,
        cantidad_abierta=1.0,
    )
    orden.pnl_realizado = 100.0

    manager = MagicMock()
    manager.bus = None
    manager.modo_real = False
    manager.historial = {}
    manager.max_historial = 50
    manager.ordenes = {"ETH/USDT": orden}
    manager._registro_pendiente_paused = set()
    manager._set_latent_pnl = MagicMock()
    manager._emit_pnl_update = MagicMock()
    manager._publish_registrar_perdida = AsyncMock()
    manager._actualizar_capital_disponible = MagicMock()

    await _finalizar_cierre_completo_async(
        manager, "ETH/USDT", orden, 2100.0, "Take Profit", "op-1"
    )

    archivo = historial_operaciones.CARPETA_ORDENES / "ETH_USDT.parquet"
    assert archivo.exists(), "el cierre debe alimentar el parquet de aprendizaje"
    df = pd.read_parquet(archivo)
    assert df["retorno_total"].iloc[-1] == pytest.approx(0.05)
    assert json.loads(df["estrategias_activas"].iloc[-1]) == {"rsi": 1.0}


@pytest.mark.asyncio
async def test_run_registra_tarea_aprendizaje_si_flag_activo(trader_factory) -> None:
    trader = trader_factory(["BTC/USDT"])
    trader.config.aprendizaje_continuo_enabled = True
    trader.config.aprendizaje_intervalo_sec = 3600
    trader._stop_event.set()
    await trader._run()
    nombres = [t.get("name") for t in trader.supervisor.supervised]
    assert "aprendizaje_continuo" in nombres


@pytest.mark.asyncio
async def test_run_sin_flag_no_registra_tarea_aprendizaje(trader_factory) -> None:
    trader = trader_factory(["BTC/USDT"])
    trader._stop_event.set()
    await trader._run()
    nombres = [t.get("name") for t in trader.supervisor.supervised]
    assert "aprendizaje_continuo" not in nombres
