import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd
from tqdm import tqdm

from config.config_manager import Config
from core.trader_simulado import TraderSimulado


@dataclass
class BacktestConfig:
    symbols: list[str]
    data_dir: str = "datos"
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    initial_capital: float = 1000.0
    intervalo: str = "1m"


class BacktestRunner:
    """Ejecución simplificada y eficiente de backtests."""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self.data: Dict[str, pd.DataFrame] = {}
        self._load_data()
        trader_cfg = Config(
            api_key="",
            api_secret="",
            modo_real=False,
            intervalo_velas=self.config.intervalo,
            symbols=self.config.symbols,
            umbral_riesgo_diario=0.05,
            min_order_eur=10.0,
            persistencia_minima=1,
        )
        self.trader = TraderSimulado(trader_cfg)

    def _load_data(self) -> None:
        for sym in self.config.symbols:
            path = f"{self.config.data_dir}/{sym.replace('/', '_').lower()}_{self.config.intervalo}.parquet"
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Datos no encontrados para {sym}: {path}")
            df = pd.read_parquet(path).dropna().sort_values("timestamp").reset_index(drop=True)
            if self.config.start is not None:
                df = df[df["timestamp"] >= pd.Timestamp(self.config.start)]
            if self.config.end is not None:
                df = df[df["timestamp"] <= pd.Timestamp(self.config.end)]
            self.data[sym] = df.reset_index(drop=True)
        if not self.data:
            raise ValueError("No se cargaron datos de históricos")

    async def run(self) -> TraderSimulado:
        max_len = max(len(df) for df in self.data.values())
        with tqdm(total=max_len, desc="Procesando") as bar:
            for i in range(max_len):
                tareas = []
                for sym, df in self.data.items():
                    if i >= len(df):
                        continue
                    row = df.iloc[i]
                    vela = {
                        "symbol": sym,
                        "timestamp": row["timestamp"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                    }
                    tareas.append(self.trader.procesar_vela(vela))
                if tareas:
                    await asyncio.gather(*tareas)
                    bar.update(1)
        await self.trader.cerrar()
        return self.trader


def run_backtest(config: BacktestConfig) -> TraderSimulado:
    return asyncio.run(BacktestRunner(config).run())