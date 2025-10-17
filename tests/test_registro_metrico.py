from __future__ import annotations

import os

import core.registro_metrico as registro_module
from core.registro_metrico import RegistroMetrico
from observability.metrics import METRIC_EXPORT_FAILURES_TOTAL


def _clear_metric_env(monkeypatch) -> None:
    for var in (
        "METRIC_DEFAULT_SYMBOL",
        "METRIC_DEFAULT_ESTRATEGIA",
        "METRIC_DEFAULT_EXCHANGE",
        "METRIC_DEFAULT_ORDER_TYPE",
        "METRIC_DEFAULT_MODO",
        "METRIC_DEFAULT_LATENCIA_MS",
        "MODO_REAL",
    ):
        monkeypatch.delenv(var, raising=False)


def test_registro_metrico_aplica_defaults(tmp_path, monkeypatch) -> None:
    _clear_metric_env(monkeypatch)
    monkeypatch.setenv("MODO_REAL", "false")
    registro = RegistroMetrico(carpeta=os.fspath(tmp_path))

    registro.registrar("evento", {"symbol": "BTCUSDT"})

    stored = registro.buffer[-1]
    assert stored["symbol"] == "BTCUSDT"
    assert stored["estrategia"] == "default"
    assert stored["exchange"] == "binance"
    assert stored["order_type"] == "n/a"
    assert stored["modo"] == "paper"
    assert stored["latencia_ms"] == 0.0


def test_registro_metrico_acepta_aliases(tmp_path, monkeypatch) -> None:
    _clear_metric_env(monkeypatch)
    monkeypatch.setenv("METRIC_DEFAULT_MODO", "paper")
    registro = RegistroMetrico(carpeta=os.fspath(tmp_path))

    registro.registrar(
        "evento",
        {
            "ticker": "ETHUSDT",
            "strategy": "scalp",
            "venue": "binance-futures",
            "orderType": "LIMIT",
            "latency_ms": "12.5",
        },
    )

    stored = registro.buffer[-1]
    assert stored["symbol"] == "ETHUSDT"
    assert stored["estrategia"] == "scalp"
    assert stored["exchange"] == "binance-futures"
    assert stored["order_type"] == "LIMIT"
    assert stored["modo"] == "paper"
    assert stored["latencia_ms"] == 12.5


def test_registro_metrico_normaliza_latencia_invalida(tmp_path, monkeypatch) -> None:
    _clear_metric_env(monkeypatch)
    monkeypatch.setenv("METRIC_DEFAULT_SYMBOL", "N/A")
    monkeypatch.setenv("METRIC_DEFAULT_LATENCIA_MS", "5")
    registro = RegistroMetrico(carpeta=os.fspath(tmp_path))

    registro.registrar("evento", {"latencia_ms": -3})

    stored = registro.buffer[-1]
    assert stored["symbol"] == "N/A"
    assert stored["latencia_ms"] == 5.0


def test_exportar_incrementa_metricas_en_reintentos(tmp_path, monkeypatch) -> None:
    registro = RegistroMetrico(carpeta=os.fspath(tmp_path))
    registro.registrar(
        "evento",
        {
            "symbol": "BTCUSDT",
            "estrategia": "scalp",
            "exchange": "binance",
            "order_type": "LIMIT",
            "modo": "real",
            "latencia_ms": 12.0,
        },
    )

    def _raise_io(*_args, **_kwargs):
        raise IOError("boom")

    monkeypatch.setattr("core.registro_metrico.observe_disk_write", _raise_io)
    monkeypatch.setattr(registro_module.time, "sleep", lambda *_: None)

    metric = METRIC_EXPORT_FAILURES_TOTAL.labels(operation="registro_metrico_csv")
    initial = getattr(metric, "_value", 0.0)

    registro.exportar(intentos=2)

    assert metric._value == initial + 2
