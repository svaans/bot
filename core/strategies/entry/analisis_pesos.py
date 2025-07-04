import os
import pandas as pd
import json
from collections import defaultdict
from core.strategies.pesos import gestor_pesos


def calcular_pesos_desde_backtest(csv_path=
    'ordenes_simuladas_con_resultado.csv', output_json=
    'estrategias_pesos_backtest.json'):
    df = pd.read_csv(csv_path)
    df = df[df['status'] == 'cerrada'].copy()
    df['ganancia'] = df['precio_salida'] - df['precio_entrada']

    def limpiar_estrategias(x):
        if not isinstance(x, str):
            return {}
        try:
            return json.loads(x)
        except json.JSONDecodeError:
            return {}
    df['estrategias_activas'] = df['estrategias_activas'].apply(
        limpiar_estrategias)
    resultados_por_symbol = defaultdict(lambda : defaultdict(lambda : {
        'usos': 0, 'ganadoras': 0, 'ganancia_total': 0}))
    for _, row in df.iterrows():
        estrategias_activas = row['estrategias_activas']
        symbol = row['symbol']
        for estrategia, valor in estrategias_activas.items():
            if valor:
                stats = resultados_por_symbol[symbol][estrategia]
                stats['usos'] += 1
                stats['ganancia_total'] += row['ganancia']
                if row['ganancia'] > 0:
                    stats['ganadoras'] += 1
    pesos_por_symbol = {}
    for symbol, estrategias in resultados_por_symbol.items():
        pesos_por_symbol[symbol] = {}
        for estrategia, stats in estrategias.items():
            if stats['usos'] >= 3:
                winrate = stats['ganadoras'] / stats['usos']
                media_ganancia = stats['ganancia_total'] / stats['usos']
                peso = round(winrate * media_ganancia + 1, 2)
                pesos_por_symbol[symbol][estrategia] = max(peso, 0.1)
    if os.path.exists(output_json):
        with open(output_json, 'r') as f:
            datos_anteriores = json.load(f)
    else:
        datos_anteriores = {}
    datos_anteriores.update(pesos_por_symbol)
    with open(output_json, 'w') as f:
        json.dump(datos_anteriores, f, indent=4)
    gestor_pesos.guardar(datos_anteriores)
    print('✅ Pesos generados y guardados por símbolo en', output_json)
    return datos_anteriores
