import logging
import pandas as pd
from core.strategies.entry.gestor_entradas import entrada_permitida
from core.strategies.exit.salida_stoploss import verificar_salida_stoploss
from _pytest.logging import LogCaptureHandler


def test_log_entrada_rechazada(caplog):
    df = pd.DataFrame({"close": [1]*30, "high": [1]*30, "low": [1]*30, "volume": [100]*30})
    estrategias = {"s1": True}
    logger = logging.getLogger("entradas")
    handler = LogCaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    permitido = entrada_permitida(
        "AAA", 0.5, 1.0, estrategias, 50, 0.1, 0.1, df, cantidad=1
    )
    logger.removeHandler(handler)
    assert not permitido
    mensajes = [r.getMessage() for r in handler.records]
    assert any("score_check_failed" in m for m in mensajes)
    assert any("entrada_rechazada" in m for m in mensajes)


def test_log_sl_evitar(monkeypatch, caplog):
    df = pd.DataFrame({"close": [90]*30, "high": [91]*30, "low": [89]*30, "volume": [100]*30})
    orden = {"symbol": "AAA", "precio_entrada": 100, "stop_loss": 90, "direccion": "long", "sl_evitar_info": [], "duracion_en_velas": 0}

    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.detectar_tendencia",
        lambda symbol, df: ("alcista", None),
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.evaluar_estrategias",
        lambda symbol, df, tendencia: {
            "estrategias_activas": {
                "ascending_scallop": True,
                "ascending_triangle": True,
                "cup_with_handle": True,
            },
            "puntaje_total": 5,
        },
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_umbral_adaptativo",
        lambda *a, **k: 2,
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_umbral_salida_adaptativo",
        lambda *a, **k: 1,
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_rsi", lambda df: 50
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_slope", lambda df: 0.1
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_momentum", lambda df: 0.1
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.validar_sl_tecnico",
        lambda df, direccion="long": 0.0,
    )

    logger = logging.getLogger("salida_stoploss")
    handler = LogCaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    res = verificar_salida_stoploss(orden, df, config={})
    logger.removeHandler(handler)
    assert res["evitado"] is True
    assert any("sl_evitar" in r.getMessage() for r in handler.records)


def test_sl_evitar_info_none(monkeypatch):
    """No debe fallar cuando sl_evitar_info es None."""
    df = pd.DataFrame({"close": [90] * 30, "high": [91] * 30, "low": [89] * 30})
    orden = {
        "symbol": "AAA",
        "precio_entrada": 100,
        "stop_loss": 90,
        "direccion": "long",
        "sl_evitar_info": None,
        "duracion_en_velas": 0,
    }

    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.detectar_tendencia",
        lambda symbol, df: ("alcista", None),
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.evaluar_estrategias",
        lambda symbol, df, tendencia: {
            "estrategias_activas": {"dummy": True},
            "puntaje_total": 5,
        },
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_umbral_adaptativo",
        lambda *a, **k: 2,
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_umbral_salida_adaptativo",
        lambda *a, **k: 1,
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_rsi", lambda df: 50
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_slope", lambda df: 0.1
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.calcular_momentum", lambda df: 0.1
    )
    monkeypatch.setattr(
        "core.strategies.exit.salida_stoploss.validar_sl_tecnico",
        lambda df, direccion="long": 0.0,
    )

    res = verificar_salida_stoploss(orden, df, config={})
    assert "cerrar" in res  # No debe lanzar TypeError por len(None)