import json
import pandas as pd
import pytest
from learning.analisis_resultados import analizar_estrategias_en_ordenes
from learning.entrenador_estrategias import evaluar_estrategias
from learning.utils_resultados import distribuir_retorno_por_estrategia


def test_distribuir_retorno_por_pesos():
    contribuciones = distribuir_retorno_por_estrategia(9.0, {
        "tendencia": 2,
        "rsi": 1,
    })
    assert contribuciones.keys() == {"tendencia", "rsi"}
    assert contribuciones["tendencia"] == pytest.approx(6.0)
    assert contribuciones["rsi"] == pytest.approx(3.0)


def test_distribuir_retorno_equidad_bool():
    contribuciones = distribuir_retorno_por_estrategia(2.0, {
        "ema": True,
        "macd": True,
        "rsi": False,
    })
    assert contribuciones.keys() == {"ema", "macd"}
    assert contribuciones["ema"] == pytest.approx(1.0)
    assert contribuciones["macd"] == pytest.approx(1.0)


def test_evaluar_estrategias_divide_retorno():
    df = pd.DataFrame([
        {"estrategias_activas": {"ema": True, "macd": True}, "retorno_total": 1.0},
        {"estrategias_activas": json.dumps({"ema": True}), "retorno": -0.2},
    ])

    resultado = evaluar_estrategias(df)

    assert resultado["ema"] == pytest.approx([0.5, -0.2])
    assert resultado["macd"] == pytest.approx([0.5])


def test_analizar_estrategias_usa_retorno_total(tmp_path):
    pytest.importorskip("pyarrow", reason="pyarrow requerido para parquet en pruebas")
    ruta = tmp_path / "ordenes.parquet"
    df = pd.DataFrame([
        {"estrategias_activas": json.dumps({"trend": True, "rsi": True}), "retorno_total": 1.0, "resultado": "ganancia"},
        {"estrategias_activas": json.dumps({"trend": True}), "retorno_total": -0.6, "resultado": "perdida"},
    ])
    df.to_parquet(ruta, index=False)

    metricas = analizar_estrategias_en_ordenes(str(ruta))

    assert set(metricas["estrategia"]) == {"trend", "rsi"}
    trend = metricas.loc[metricas["estrategia"] == "trend"].iloc[0]
    rsi = metricas.loc[metricas["estrategia"] == "rsi"].iloc[0]

    assert trend["retorno_total"] == pytest.approx(-0.1)
    assert trend["total"] == 2
    assert rsi["retorno_total"] == pytest.approx(0.5)