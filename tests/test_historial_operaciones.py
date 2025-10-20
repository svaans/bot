"""Pruebas para el cargador consolidado de historiales de operaciones."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from learning import historial_operaciones as loader


def _instalar_stub_parquet(
    monkeypatch: pytest.MonkeyPatch,
    datasets: Dict[Path, pd.DataFrame | Exception],
) -> None:
    """Instala un ``read_parquet`` de pruebas respaldado por ``datasets``."""

    def _fake_read_parquet(path: str | Path) -> pd.DataFrame:
        ruta = Path(path).resolve()
        if ruta not in datasets:
            raise FileNotFoundError(ruta)
        contenido = datasets[ruta]
        if isinstance(contenido, Exception):
            raise contenido
        return contenido.copy()

    monkeypatch.setattr(loader.pd, "read_parquet", _fake_read_parquet)


def test_cargar_historial_prefiere_fuente_oficial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "BTC/USDT"
    oficial = tmp_path / "ordenes_simuladas"
    espejo = tmp_path / "learning" / "ultimas_operaciones"
    monkeypatch.setattr(loader, "CARPETA_ORDENES", oficial)
    monkeypatch.setattr(loader, "CARPETA_ULTIMAS", espejo)

    oficial_archivo = oficial / "BTC_USDT.parquet"
    espejo_archivo = espejo / "BTC_USDT.parquet"
    oficial_archivo.parent.mkdir(parents=True, exist_ok=True)
    espejo_archivo.parent.mkdir(parents=True, exist_ok=True)
    oficial_archivo.touch()
    espejo_archivo.touch()

    datasets = {
        oficial_archivo.resolve(): pd.DataFrame(
            {"timestamp": [1, 2], "retorno_total": [0.1, 0.2]}
        ),
        espejo_archivo.resolve(): pd.DataFrame(
            {"timestamp": [1], "retorno_total": [0.5]}
        ),
    }
    _instalar_stub_parquet(monkeypatch, datasets)

    resultado = loader.cargar_historial_operaciones(symbol, max_operaciones=1)

    assert resultado.source == oficial_archivo.resolve()
    assert resultado.data['timestamp'].tolist() == [2]


def test_cargar_historial_usa_respaldo_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "ETH/USDT"
    oficial = tmp_path / "ordenes_simuladas"
    espejo = tmp_path / "learning" / "ultimas_operaciones"
    monkeypatch.setattr(loader, "CARPETA_ORDENES", oficial)
    monkeypatch.setattr(loader, "CARPETA_ULTIMAS", espejo)

    espejo_archivo = espejo / "ETH_USDT.parquet"
    espejo_archivo.parent.mkdir(parents=True, exist_ok=True)
    espejo_archivo.touch()

    datasets = {
        espejo_archivo.resolve(): pd.DataFrame(
            {"timestamp": [10, 20], "retorno_total": [-0.1, 0.3]}
        )
    }
    _instalar_stub_parquet(monkeypatch, datasets)

    resultado = loader.cargar_historial_operaciones(symbol)

    assert resultado.source == espejo_archivo.resolve()
    assert resultado.data['retorno_total'].tolist() == [-0.1, 0.3]


def test_cargar_historial_descarta_archivo_corrupto(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "XRP/USDT"
    oficial = tmp_path / "ordenes_simuladas"
    espejo = tmp_path / "learning" / "ultimas_operaciones"
    monkeypatch.setattr(loader, "CARPETA_ORDENES", oficial)
    monkeypatch.setattr(loader, "CARPETA_ULTIMAS", espejo)

    oficial_archivo = oficial / "XRP_USDT.parquet"
    oficial_archivo.parent.mkdir(parents=True, exist_ok=True)
    oficial_archivo.touch()
    espejo_archivo = espejo / "XRP_USDT.parquet"
    espejo_archivo.parent.mkdir(parents=True, exist_ok=True)
    espejo_archivo.touch()

    datasets = {
        oficial_archivo.resolve(): RuntimeError("archivo corrupto"),
        espejo_archivo.resolve(): pd.DataFrame(
            {"timestamp": [5], "retorno_total": [0.7]}
        ),
    }
    _instalar_stub_parquet(monkeypatch, datasets)

    resultado = loader.cargar_historial_operaciones(symbol)

    assert resultado.source == espejo_archivo.resolve()
    assert resultado.data['retorno_total'].tolist() == [0.7]


def test_cargar_historial_sin_fuentes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "ADA/USDT"
    oficial = tmp_path / "ordenes_simuladas"
    espejo = tmp_path / "learning" / "ultimas_operaciones"
    monkeypatch.setattr(loader, "CARPETA_ORDENES", oficial)
    monkeypatch.setattr(loader, "CARPETA_ULTIMAS", espejo)

    _instalar_stub_parquet(monkeypatch, {})

    with pytest.raises(FileNotFoundError):
        loader.cargar_historial_operaciones(symbol)
