"""Backtesting con validación por ventanas cronológicas."""
import asyncio
from typing import Dict, Iterable, List, Tuple
import pandas as pd
import optuna
from core.trader_simulado import TraderSimulado


def cargar_historicos(symbols: Iterable[str], ruta_datos: str='datos') ->Dict[
    str, pd.DataFrame]:
    datos = {}
    for s in symbols:
        archivo = f"{ruta_datos}/{s.replace('/', '_').lower()}_1m.parquet"
        df = pd.read_parquet(archivo).dropna().sort_values('timestamp'
            ).reset_index(drop=True)
        datos[s] = df
    return datos


def dividir_ventanas(df: pd.DataFrame, n_ventanas: int) ->List[pd.DataFrame]:
    tam = len(df) // n_ventanas
    ventanas = []
    for i in range(n_ventanas):
        inicio = i * tam
        fin = (i + 1) * tam if i < n_ventanas - 1 else len(df)
        ventanas.append(df.iloc[inicio:fin].reset_index(drop=True))
    return ventanas


async def procesar_df(bot: TraderSimulado, symbol: str, df: pd.DataFrame
    ) ->None:
    for fila in df.itertuples():
        vela = {'symbol': symbol, 'timestamp': fila.timestamp, 'open': fila
            .open, 'high': fila.high, 'low': fila.low, 'close': fila.close,
            'volume': fila.volume}
        await bot.procesar_vela(vela)


def simular_df(df: pd.DataFrame, symbol: str, config: dict) ->float:
    bot = TraderSimulado([symbol], configuraciones={symbol: config},
        modo_optimizacion=True)
    asyncio.run(procesar_df(bot, symbol, df))
    return bot.capital_simulado[symbol]


def optimizar_config(df: pd.DataFrame, symbol: str, n_trials: int=20) ->dict:

    def objective(trial: optuna.Trial) ->float:
        config = {'sl_ratio': trial.suggest_float('sl_ratio', 1.0, 6.0),
            'tp_ratio': trial.suggest_float('tp_ratio', 1.0, 6.0)}
        capital = simular_df(df, symbol, config)
        return capital
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials)
    return study.best_params


def backtest_en_ventanas(symbol: str, n_ventanas: int=3, n_trials: int=20
    ) ->List[Tuple[dict, float]]:
    historico = cargar_historicos([symbol])[symbol]
    ventanas = dividir_ventanas(historico, n_ventanas)
    resultados = []
    for i in range(len(ventanas) - 1):
        train = ventanas[i]
        test = ventanas[i + 1]
        best_config = optimizar_config(train, symbol, n_trials)
        capital = simular_df(test, symbol, best_config)
        resultados.append((best_config, capital))
        print(f'🪟 Ventana {i + 1}/{n_ventanas - 1} — Capital {capital:.2f}')
    return resultados


if __name__ == '__main__':
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'BTC/EUR'
    resultados = backtest_en_ventanas(symbol)
    for idx, (config, capital) in enumerate(resultados, 1):
        print(f'\nVentana {idx}: Capital final {capital:.2f}')
        print(config)
