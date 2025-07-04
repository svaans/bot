import json
import pandas as pd
from collections import defaultdict


def analizar_estrategias_en_ordenes(path_ordenes: str, dias: (int | None)=None
    ) ->pd.DataFrame:
    """Devuelve métricas por estrategia a partir de ``path_ordenes``.

    Si ``dias`` se especifica se filtra el ``DataFrame`` a ese rango temporal
    utilizando la columna ``timestamp`` si está disponible.
    """
    try:
        df = pd.read_parquet(path_ordenes)
    except Exception as e:
        print(f'❌ Error al leer el archivo de órdenes: {e}')
        return pd.DataFrame()
    if dias is not None and 'timestamp' in df.columns:
        limite = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days
            =dias)
        ts = pd.to_datetime(df['timestamp'], unit='s', errors='coerce'
            ).dt.tz_localize(None)
        df = df[ts >= limite]
    conteo = defaultdict(lambda : {'ganadas': 0, 'perdidas': 0, 'total': 0,
        'retorno': 0.0})
    for _, fila in df.iterrows():
        estrategias_activas = json.loads(fila.get('estrategias_activas', '{}'))
        retorno = fila.get('retorno', 0)
        resultado = fila.get('resultado', '')
        for estrategia, activa in estrategias_activas.items():
            if activa:
                conteo[estrategia]['total'] += 1
                conteo[estrategia]['retorno'] += retorno
                if resultado == 'ganancia':
                    conteo[estrategia]['ganadas'] += 1
                elif resultado == 'perdida':
                    conteo[estrategia]['perdidas'] += 1
    datos = []
    for estrategia, stats in conteo.items():
        winrate = stats['ganadas'] / stats['total'] * 100 if stats['total'
            ] > 0 else 0
        promedio = stats['retorno'] / stats['total'] if stats['total'
            ] > 0 else 0
        datos.append({'estrategia': estrategia, 'ganadas': stats['ganadas'],
            'perdidas': stats['perdidas'], 'total': stats['total'],
            'winrate': round(winrate, 2), 'retorno_promedio': round(
            promedio, 5), 'retorno_total': round(stats['retorno'], 5)})
    return pd.DataFrame(datos).sort_values(by='retorno_total', ascending=False)


if __name__ == '__main__':
    df_metricas = analizar_estrategias_en_ordenes('ordenes_reales.parquet')
    print('\n📊 Métricas por estrategia:')
    print(df_metricas.to_string(index=False))
