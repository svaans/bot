import os
import numpy as np
import pandas as pd
from typing import Optional

UMBRAL_HEURISTIC_BASE = float(os.getenv("UMBRAL_HEURISTIC_BASE", 6.0))

try:
    from stable_baselines3 import PPO
except Exception:  # pragma: no cover - library optional
    PPO = None

from core.logger import configurar_logger

log = configurar_logger("rl_policy")


class RLPolicy:
    """Carga y utiliza un modelo de stable-baselines3 para estimar el umbral."""

    def __init__(self, path: str = "aprendizaje/umbral_rl.zip") -> None:
        self.path = path
        self.model = None
        self._load_model()

    def _load_model(self) -> None:
        if PPO is None:
            log.warning("stable-baselines3 no está instalado")
            return
        if os.path.exists(self.path):
            try:
                self.model = PPO.load(self.path)
                log.info(f"Modelo RL cargado desde {self.path}")
            except Exception as e:  # pragma: no cover - carga fallida
                log.warning(f"No se pudo cargar el modelo RL: {e}")
        else:
            log.warning(f"Modelo RL no encontrado en {self.path}")

    def _features_from_df(self, df: pd.DataFrame) -> Optional[np.ndarray]:
        if df is None or len(df) < 30:
            return None
        ventana_close = df["close"].tail(10)
        ventana_high = df["high"].tail(10)
        ventana_low = df["low"].tail(10)
        ventana_vol = df["volume"].tail(30)

        media_close = np.mean(ventana_close)
        if media_close == 0 or np.isnan(media_close):
            return None
        volatilidad = np.std(ventana_close) / media_close
        rango_medio = np.mean(ventana_high - ventana_low) / media_close
        volumen_promedio = ventana_vol.mean()
        volumen_max = ventana_vol.max()
        volumen_relativo = 0.0 if volumen_max == 0 else volumen_promedio / volumen_max
        momentum_std = df["close"].pct_change().tail(5).std()
        try:
            from scipy.stats import linregress

            slope = linregress(range(len(ventana_close)), ventana_close).slope
        except Exception:
            slope = 0.0
        try:
            from ta.momentum import RSIIndicator

            rsi = RSIIndicator(close=df["close"], window=14).rsi().iloc[-1]
        except Exception:
            rsi = 50.0
        return np.array([
            volatilidad,
            rango_medio,
            volumen_relativo,
            momentum_std,
            slope,
            rsi,
        ], dtype=np.float32)

    def _heuristic_umbral(self, features: np.ndarray) -> float:
        volatilidad, rango_medio, volumen_relativo, momentum_std, slope, rsi = features
        umbral = UMBRAL_HEURISTIC_BASE
        umbral += volatilidad * 4.0
        umbral += rango_medio * 2.5
        umbral += volumen_relativo * 2.0
        umbral += momentum_std * 3.0
        umbral += slope * 1.5
        if 45.0 <= rsi <= 55.0:
            umbral *= 0.9
        return float(np.clip(umbral, 3.0, 20.0))

    def sugerir_umbral(self, df: pd.DataFrame) -> Optional[float]:
        override = os.getenv("RL_ACTION")
        if override is not None:
            try:
                return float(override)
            except ValueError:
                log.warning("Valor RL_ACTION inválido, ignorando override")
        features = self._features_from_df(df)
        if features is None:
            return None
        if self.model is None:
            log.debug("Usando heurística de umbral por ausencia de modelo RL")
            return self._heuristic_umbral(features)
        try:
            action, _ = self.model.predict(features, deterministic=True)
            return float(action)
        except Exception as e:  # pragma: no cover - predicción fallida
            log.warning(f"No se pudo predecir umbral RL: {e}")
            return self._heuristic_umbral(features)



rl_policy = RLPolicy()
