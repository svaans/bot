import asyncio
import json
import os
from typing import Dict, Iterable, List
import numpy as np
import optuna
import pandas as pd
from core.trader_simulado import TraderSimulado
SYMBOLS = ['BTC/EUR', 'ETH/EUR', 'ADA/EUR', 'SOL/EUR', 'BNB/EUR']
RUTA_DATOS = 'datos'
N_TRIALS = 60
N_JOBS = 4
BLOQUES = 3
BUFFER_INICIAL = 30


def cargar_historicos(symbols: Iterable[str], ruta: str=RUTA_DATOS) ->Dict[
    str, pd.DataFrame]:
    """Carga los datos de mercado desde archivos Parquet."""
    datos = {}
    for s in symbols:
        archivo = f"{ruta}/{s.replace('/', '_').lower()}_1m.parquet"
        if not os.path.isfile(archivo):
            print(f'⚠️ Archivo no encontrado para {s}. Se omite.')
            continue
        df = pd.read_parquet(archivo).dropna().sort_values('timestamp'
            ).reset_index(drop=True)
        datos[s] = df
    return datos


def dividir_bloques(df: pd.DataFrame, bloques: int=BLOQUES) ->List[pd.DataFrame
    ]:
    """Divide el DataFrame en bloques temporales del mismo tamaño."""
    tam = len(df) // bloques
    partes = []
    for i in range(bloques):
        inicio = i * tam
        fin = (i + 1) * tam if i < bloques - 1 else len(df)
        partes.append(df.iloc[inicio:fin].reset_index(drop=True))
    return partes


async def procesar_df(bot: TraderSimulado, symbol: str, df: pd.DataFrame
    ) ->None:
    """Alimenta el bot con velas una a una."""
    for fila in df.itertuples():
        vela = {'symbol': symbol, 'timestamp': fila.timestamp, 'open': fila
            .open, 'high': fila.high, 'low': fila.low, 'close': fila.close,
            'volume': fila.volume}
        await bot.procesar_vela(vela)


async def ejecutar_backtest(symbol: str, dfs: List[pd.DataFrame], config:
    dict, pesos: dict) ->TraderSimulado:
    """Ejecuta el backtest para un símbolo con su configuración y pesos."""
    bot = TraderSimulado([symbol], configuraciones={symbol: config},
        pesos_personalizados={symbol: pesos}, modo_optimizacion=True)
    for df in dfs:
        await procesar_df(bot, symbol, df)
    await bot.cerrar()
    return bot


def evaluar(bot: TraderSimulado, symbol: str) ->dict:
    """Calcula métricas de rendimiento básicas."""
    historial = bot.resultados.get(symbol, [])
    capital_final = bot.capital_simulado.get(symbol, 0)
    capital_inicial = 1000.0
    if not historial:
        return {'capital': capital_final, 'ganancia': capital_final -
            capital_inicial, 'winrate': 0.0, 'drawdown': 0.0, 'sharpe': 0.0,
            'ratio_br': 0.0, 'num_ops': 0}
    resultados = np.array(historial)
    ganancias = resultados[resultados > 0]
    perdidas = resultados[resultados <= 0]
    acumulado = resultados.cumsum()
    max_acum = np.maximum.accumulate(acumulado)
    drawdown = float((max_acum - acumulado).max()) if len(acumulado) else 0.0
    sharpe = resultados.mean() / resultados.std() * np.sqrt(len(resultados)
        ) if resultados.std() > 0 else 0.0
    ratio_br = ganancias.mean() / -perdidas.mean() if len(perdidas) and len(
        ganancias) else 0.0
    return {'capital': capital_final, 'ganancia': capital_final -
        capital_inicial, 'winrate': len(ganancias) / len(resultados) * 100,
        'drawdown': drawdown, 'sharpe': sharpe, 'ratio_br': ratio_br,
        'num_ops': len(resultados)}


def optimizar_symbol(symbol: str, df: pd.DataFrame, estrategias: List[str]
    ) ->tuple[dict, dict, dict]:
    """Ejecuta la optimización para un símbolo."""
    bloques = dividir_bloques(df, BLOQUES)

    def objective(trial: optuna.Trial) ->float:
        config_symbol = {'factor_umbral': trial.suggest_float(
            'factor_umbral', 0.7, 2.0), 'ajuste_volatilidad': trial.
            suggest_float('ajuste_volatilidad', 0.0, 1.0),
            'riesgo_maximo_diario': trial.suggest_float(
            'riesgo_maximo_diario', 0.01, 0.1), 'peso_minimo_total': trial.
            suggest_float('peso_minimo_total', 0.5, 3.0),
            'diversidad_minima': trial.suggest_int('diversidad_minima', 1, 
            5), 'cooldown_tras_perdida': trial.suggest_int(
            'cooldown_tras_perdida', 1, 10), 'sl_ratio': trial.
            suggest_float('sl_ratio', 0.002, 0.03), 'tp_ratio': trial.
            suggest_float('tp_ratio', 0.002, 0.04),
            'ratio_minimo_beneficio': trial.suggest_float(
            'ratio_minimo_beneficio', 1.0, 2.5),
            'multiplicador_estrategias_recurrentes': trial.suggest_float(
            'multiplicador_estrategias_recurrentes', 0.0, 2.0),
            'ponderar_por_diversidad': trial.suggest_categorical(
            'ponderar_por_diversidad', [True, False]), 'modo_agresivo':
            trial.suggest_categorical('modo_agresivo', [True, False])}
        pesos = {e: trial.suggest_float(f'peso_{e}', 0.0, 2.0) for e in
            estrategias}
        bot = asyncio.run(ejecutar_backtest(symbol, bloques, config_symbol,
            pesos))
        metricas = evaluar(bot, symbol)
        trial.set_user_attr('metricas', metricas)
        return metricas['capital']
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=N_TRIALS, n_jobs=N_JOBS)
    return study.best_params, study.best_trial.user_attrs['metricas'], {k.
        replace('peso_', ''): v for k, v in study.best_params.items() if k.
        startswith('peso_')}


def guardar_resultados(configs: dict, pesos: dict, metricas: dict) ->None:
    """Almacena configuraciones, pesos y métricas en disco."""
    with open('configuraciones_optimas.json', 'w') as f:
        json.dump(configs, f, indent=4)
    with open('estrategias_pesos_optimos.json', 'w') as f:
        json.dump(pesos, f, indent=4)
    with open('informe_detallado.txt', 'w') as f:
        for sym, m in metricas.items():
            f.write(f'\n📊 {sym}\n')
            f.write(f"Ganancia neta: {m['ganancia']:.2f}\n")
            f.write(f"Winrate: {m['winrate']:.2f}%\n")
            f.write(f"Drawdown: {m['drawdown']:.2f}\n")
            f.write(f"Sharpe Ratio: {m['sharpe']:.2f}\n")
            f.write(f"Ratio B/R: {m['ratio_br']:.2f}\n")
            f.write(f"Operaciones: {m['num_ops']}\n")


if __name__ == '__main__':
    datos = cargar_historicos(SYMBOLS)
    if not datos:
        raise SystemExit("No se encontraron históricos en la carpeta 'datos'")
    with open('config/estrategias_pesos.json', 'r') as f:
        pesos_base = json.load(f)
    estrategias = list(next(iter(pesos_base.values())).keys()
        ) if pesos_base else []
    configuraciones, pesos_optimos, metricas_finales = {}, {}, {}
    for symbol, df in datos.items():
        print(f'\n🔍 Optimizando {symbol}...')
        config, metricas, pesos = optimizar_symbol(symbol, df, estrategias)
        configuraciones[symbol] = {k: v for k, v in config.items() if not k
            .startswith('peso_')}
        pesos_optimos[symbol] = pesos
        metricas_finales[symbol] = metricas
    guardar_resultados(configuraciones, pesos_optimos, metricas_finales)
    print('\n✅ Optimización completa. Resultados guardados.')
