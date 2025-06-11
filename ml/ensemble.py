import os
import joblib
import numpy as np
from sklearn.ensemble import VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC


class EnsembleModel:
    """Carga y utiliza un modelo ensemble para estimar probabilidades."""

    def __init__(self, path: str = "ml/model.joblib") -> None:
        self.path = path
        self.model = None
        self._load_model()

    def _default_model(self) -> VotingClassifier:
        estimators = [
            ("lr", LogisticRegression(max_iter=100)),
            ("rf", RandomForestClassifier()),
            ("svc", SVC(probability=True)),
        ]
        return VotingClassifier(estimators=estimators, voting="soft")

    def _load_model(self) -> None:
        if os.path.exists(self.path):
            try:
                self.model = joblib.load(self.path)
            except Exception as e:
                print(f"⚠️ No se pudo cargar el modelo desde {self.path}: {e}")
                self.model = self._default_model()
        else:
            self.model = self._default_model()

    def predict_proba(self, vector) -> float:
        if self.model is None:
            return 0.5
        arr = np.array(vector).reshape(1, -1)
        try:
            proba = self.model.predict_proba(arr)[0][1]
        except Exception:
            pred = self.model.predict(arr)[0]
            proba = float(pred)
        return float(proba)


ensemble_model = EnsembleModel()