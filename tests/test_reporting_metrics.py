import os

import pytest

from core.reporting import ReporterDiario
from observability.metrics import REPORT_IO_ERRORS_TOTAL


def test_reporter_registrar_operacion_incrementa_metricas(tmp_path, monkeypatch) -> None:
    reporter = ReporterDiario(carpeta=os.fspath(tmp_path))

    def _raise_io(*_args, **_kwargs):
        raise IOError("fail")

    monkeypatch.setattr("core.reporting.observe_disk_write", _raise_io)

    metric = REPORT_IO_ERRORS_TOTAL.labels(operation="report_operacion_csv_create")
    initial = getattr(metric, "_value", 0.0)

    with pytest.raises(IOError):
        reporter.registrar_operacion({"symbol": "BTCUSDT", "retorno_total": 0.0})

    assert metric._value == initial + 1