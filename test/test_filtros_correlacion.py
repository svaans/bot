import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from core.strategies.entry.validador_entradas import verificar_liquidez_orden
from core.strategies.entry.gestor_entradas import entrada_permitida


def test_verificar_liquidez_orden():
    df = pd.DataFrame({'volume': [100] * 20})
    assert not verificar_liquidez_orden(df, 30, ventana=20, factor=0.2)
    assert verificar_liquidez_orden(df, 10, ventana=20, factor=0.2)


def test_entrada_rechazada_por_correlacion():
    datos = {'close': list(range(30)), 'volume': [100] * 30}
    df1 = pd.DataFrame(datos)
    df2 = pd.DataFrame(datos)
    estrategias = {f'e{i}': (True) for i in range(5)}
    permitido = entrada_permitida('AAA', 1.0, 0.5, estrategias, 60, 1, 1,
        df1, 'long', cantidad=1, df_referencia=df2, umbral_correlacion=0.8)
    assert permitido is False
