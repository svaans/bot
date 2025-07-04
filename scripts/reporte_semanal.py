import pandas as pd
from core.metricas_semanales import metricas_semanales, metricas_tracker


def main() ->None:
    df = metricas_semanales()
    if df.empty:
        print('Sin operaciones en la última semana')
    else:
        print('\nMétricas semanales por par:\n')
        print(df.to_string(index=False))
    datos = metricas_tracker.data
    print('\nEventos acumulados:')
    print(
        f"Entradas filtradas por persistencia: {datos.get('filtradas_persistencia', 0)}"
        )
    print(f"Entradas filtradas por umbral: {datos.get('filtradas_umbral', 0)}")
    print(
        f"Entradas filtradas por diversidad: {datos.get('filtradas_diversidad', 0)}"
        )
    difs = datos.get('diferencias_umbral', [])
    if difs:
        media = sum(difs) / len(difs)
        print(f'Diferencia media umbral-puntaje: {media:.2f}')
    print(f"Salidas evitadas por stoploss: {datos.get('sl_evitas', 0)}")
    metricas_tracker.reset()


if __name__ == '__main__':
    main()
