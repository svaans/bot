import json
from collections import defaultdict

import pandas as pd

from core.repo_paths import resolve_under_repo
from core.strategies.pesos import gestor_pesos
from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights


def calcular_pesos_desde_backtest(
    csv_path: str = "ordenes_simuladas_con_resultado.csv",
    output_json: str = "estrategias_pesos_backtest.json",
):
    csv_p = resolve_under_repo(csv_path)
    out_p = resolve_under_repo(output_json)
    df = pd.read_csv(csv_p)
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
    if out_p.exists():
        with out_p.open("r", encoding="utf-8") as f:
            datos_anteriores = json.load(f)
    else:
        datos_anteriores = {}
    datos_anteriores.update(pesos_por_symbol)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with out_p.open("w", encoding="utf-8") as f:
        json.dump(datos_anteriores, f, indent=4)
    persist_entry_weights(
        gestor_pesos,
        datos_anteriores,
        source=EntryWeightSource.ANALISIS_PESOS_BACKTEST,
        detail=str(out_p),
    )
    print("✅ Pesos generados y guardados por símbolo en", out_p)
    return datos_anteriores
