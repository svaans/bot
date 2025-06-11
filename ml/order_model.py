import json
import os
import numpy as np

try:
    import lightgbm as lgb
except Exception:  # pragma: no cover - optional dependency
    lgb = None


class OrderModel:
    """Carga un modelo LightGBM entrenado para predecir probabilidades."""

    def __init__(self, path: str = "ml/models/order_model.txt") -> None:
        self.path = path
        self.meta_path = f"{path}.json"
        self.model = None
        self.features = []
        self._load_model()

    def _load_model(self) -> None:
        if lgb is None:
            print("⚠️ LightGBM no está instalado. Usando probabilidad 0.5 por defecto")
            return
        if os.path.exists(self.path):
            try:
                self.model = lgb.Booster(model_file=self.path)
            except Exception as e:  # pragma: no cover - problemas de carga
                print(f"⚠️ No se pudo cargar el modelo {self.path}: {e}")
        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, "r") as f:
                    meta = json.load(f)
                self.features = meta.get("features", [])
            except Exception as e:  # pragma: no cover - problemas de carga
                print(f"⚠️ No se pudo cargar {self.meta_path}: {e}")

    def predict_proba(self, estrategias: dict) -> float:
        """Devuelve la probabilidad de éxito según las estrategias activas."""
        if self.model is None or not self.features:
            return 0.5
        vector = [int(estrategias.get(n, False)) for n in self.features]
        arr = np.array(vector).reshape(1, -1)
        try:
            proba = self.model.predict(arr)[0]
        except Exception:  # pragma: no cover - model issues
            return 0.5
        return float(proba)


order_model = OrderModel()