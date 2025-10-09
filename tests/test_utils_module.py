import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from core.utils.utils import (
    guardar_orden_real,
    intervalo_a_segundos,
    is_valid_number,
    leer_csv_seguro,
    round_decimal,
    timestamp_alineado,
    validar_dataframe,
    validar_integridad_velas,
)


@pytest.mark.parametrize(
    "content,expected_cols",
    [
        ("a,b\n1,2\n", None),
        ("x,y\n1,2\n", 2),
    ],
)
def test_leer_csv_seguro_valido(tmp_path: Path, content: str, expected_cols: int | None) -> None:
    path = tmp_path / "data.csv"
    path.write_text(content)
    df = leer_csv_seguro(path, expected_cols=expected_cols)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_leer_csv_seguro_falla(tmp_path: Path) -> None:
    path = tmp_path / "missing.csv"
    df = leer_csv_seguro(path)
    assert df.empty

    bad = tmp_path / "bad.csv"
    bad.write_text("a,b\n1,2\n")
    df_bad = leer_csv_seguro(bad, expected_cols=3)
    assert df_bad.empty


def test_validar_dataframe() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert validar_dataframe(df, ["a"]) is True
    assert validar_dataframe(df, ["c"]) is False
    assert validar_dataframe(None, ["a"]) is False
    assert validar_dataframe(pd.DataFrame(), ["a"]) is False


@pytest.mark.parametrize(
    "intervalo,expected",
    [
        ("1m", 60),
        ("1h", 3600),
        ("1d", 86400),
        ("desconocido", 60),
    ],
)
def test_intervalo_a_segundos(intervalo: str, expected: int) -> None:
    assert intervalo_a_segundos(intervalo) == expected


@pytest.mark.parametrize(
    "timestamp,intervalo,expected",
    [
        (1_650_000_000_000, "1m", True),
        (1_650_000_000_010, "1m", False),
        ("invalid", "1m", False),
    ],
)
def test_timestamp_alineado(timestamp: int, intervalo: str, expected: bool) -> None:
    assert timestamp_alineado(timestamp, intervalo) is expected


@pytest.mark.parametrize(
    "valor,allow_none,expected",
    [
        (1, False, True),
        (float("nan"), False, False),
        (None, True, True),
        ("abc", False, False),
    ],
)
def test_is_valid_number(valor: float | None, allow_none: bool, expected: bool) -> None:
    assert is_valid_number(valor, allow_none=allow_none) is expected


def test_validar_integridad_velas_sequence() -> None:
    velas = [
        [1_650_000_000_000, 1, 2, 3],
        [1_650_000_060_000, 1, 2, 3],
    ]
    assert validar_integridad_velas(velas, "1m") is True

    velas_gap = [
        [1_650_000_000_000, 1, 2, 3],
        [1_650_000_120_000, 1, 2, 3],
    ]
    assert validar_integridad_velas(velas_gap, "1m") is False


def test_validar_integridad_velas_mapping() -> None:
    velas = [
        {"timestamp": 1_650_000_000_000},
        {"timestamp": 1_650_000_060_000},
    ]
    assert validar_integridad_velas("BTCUSDT", "1m", velas) is True

    with pytest.raises(TypeError):
        validar_integridad_velas("BTCUSDT", "1m")


def test_guardar_orden_real(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    carpeta = tmp_path / "ordenes"
    data = {"id": 1, "timestamp": datetime.now(UTC).isoformat()}
    path = guardar_orden_real("BTCUSDT", data, carpeta=carpeta)
    assert path.exists()
    contenido = path.read_text().strip().splitlines()
    assert contenido
    assert json.loads(contenido[-1]) == data


def test_round_decimal() -> None:
    assert round_decimal(1.23456, 2) == 1.23
    assert round_decimal(1.2355, 3) == 1.236
