import asyncio
import os
import random
from typing import List
import numpy as np
import gymnasium as gym

from backtesting.backtest import backtest_modular
from core.config_manager import ConfigManager
from core.configuracion import cargar_configuracion_simbolo
from core.pesos import gestor_pesos


class UmbralEnv(gym.Env):
    """Entorno para entrenar la política de umbral mediante backtesting."""

    def __init__(self, symbols: List[str], ruta_datos: str = "datos") -> None:
        super().__init__()
        self.symbols = list(symbols)
        self.ruta_datos = ruta_datos
        self.cfg = ConfigManager.load_from_env()
        self.action_space = gym.spaces.Box(low=0.0, high=30.0, shape=(1,), dtype=np.float32)
        self.observation_space = gym.spaces.Box(low=0.0, high=30.0, shape=(10,), dtype=np.float32)
        self.state = np.zeros(10, dtype=np.float32)
        self.current_symbol = random.choice(self.symbols)

    def _obtener_estado(self, symbol: str) -> np.ndarray:
        conf = cargar_configuracion_simbolo(symbol)
        pesos = gestor_pesos.obtener_pesos_symbol(symbol)
        peso_medio = float(np.mean(list(pesos.values()))) if pesos else 0.0
        return np.array(
            [
                conf.get("factor_umbral", 1.0),
                conf.get("ajuste_volatilidad", 1.0),
                conf.get("riesgo_maximo_diario", 1.0),
                conf.get("sl_ratio", 1.5),
                conf.get("tp_ratio", 3.0),
                conf.get("cooldown_tras_perdida", 3),
                peso_medio,
                float(self.cfg.persistencia_minima),
                float(self.cfg.peso_extra_persistencia),
                float(self.cfg.umbral_riesgo_diario),
            ],
            dtype=np.float32,
        )

    def reset(self, *, seed: int | None = None, options=None):
        super().reset(seed=seed)
        self.current_symbol = random.choice(self.symbols)
        self.state = self._obtener_estado(self.current_symbol)
        return self.state, {}

    def step(self, action):
        umbral = float(action[0] if isinstance(action, (list, np.ndarray)) else action)
        os.environ["RL_ACTION"] = str(umbral)
        bot = asyncio.run(backtest_modular([self.current_symbol], self.ruta_datos))
        recompensa = sum(bot.resultados.get(self.current_symbol, []))
        os.environ.pop("RL_ACTION", None)
        self.state = self._obtener_estado(self.current_symbol)
        terminated = True
        truncated = False
        info = {"symbol": self.current_symbol, "reward": recompensa}
        return self.state, recompensa, terminated, truncated, info