import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
    '..')))
import pandas as pd
from core.strategies.entry.loader import cargar_estrategias
from collections import defaultdict
from pathlib import Path
import os


def cargar_datos_ejemplo():
    archivo = Path(os.getenv('DATOS_DIR', 'datos')) / 'btc_eur_1m.parquet'
    if not archivo.exists():
        raise FileNotFoundError(f'No se encontró el archivo: {archivo}')
    df = pd.read_parquet(archivo)
    return df


def test_contador_activaciones_y_csv():
    df = cargar_datos_ejemplo()
    estrategias = cargar_estrategias()
    contador = defaultdict(int)
    for i in range(100, len(df)):
        sub_df = df.iloc[i - 100:i].copy()
        for nombre, funcion in estrategias.items():
            try:
                resultado = funcion(sub_df)
                if isinstance(resultado, dict) and resultado.get('activo', 
                    False):
                    contador[nombre] += 1
            except Exception as e:
                print(f'❌ Error en estrategia {nombre}: {str(e)}')
    print('\n📊 Activaciones por estrategia:')
    for nombre, cantidad in sorted(contador.items(), key=lambda x: -x[1]):
        print(f'➡️ {nombre}: {cantidad} activaciones')
    resultados_df = pd.DataFrame([{'estrategia': nombre, 'activaciones':
        cantidad} for nombre, cantidad in sorted(contador.items())])
    os.makedirs('resultados', exist_ok=True)
    ruta_csv = Path('resultados/activaciones_estrategias.csv')
    resultados_df.to_csv(ruta_csv, index=False)
    print(f'\n✅ Resultados guardados en: {ruta_csv.resolve()}')
    assert any(c > 0 for c in contador.values()
        ), 'Ninguna estrategia se activó'
