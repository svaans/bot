import asyncio
import numpy as np
import gymnasium as gym

from backtesting.backtest import backtest_modular


class UmbralEnv(gym.Env):
    """Entorno para entrenar la política de umbral mediante backtesting."""

    def __init__(self, symbols, ruta_datos="datos"):
        super().__init__()
        self.symbols = symbols
        self.ruta_datos = ruta_datos
        self.action_space = gym.spaces.Box(low=0.0, high=30.0, shape=(1,), dtype=np.float32)
        self.observation_space = gym.spaces.Box(low=0.0, high=1.0, shape=(1,), dtype=np.float32)
        self.state = np.zeros(1, dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self.state = np.zeros(1, dtype=np.float32)
        return self.state, {}

    def step(self, action):
        umbral = float(action[0] if isinstance(action, (list, np.ndarray)) else action)
        # Ejecuta el backtest para obtener la recompensa
        bot = asyncio.run(backtest_modular(self.symbols, self.ruta_datos))
        recompensa = sum(sum(res) for res in bot.resultados.values())
        self.state = np.array([umbral], dtype=np.float32)
        terminated = True
        truncated = False
        info = {"reward": recompensa}
        return self.state, recompensa, terminated, truncated, info