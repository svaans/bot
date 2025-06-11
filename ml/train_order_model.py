import glob
import json
import os

import pandas as pd

try:
    import lightgbm as lgb
except Exception:
    lgb = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERACIONES_DIR = os.path.join(BASE_DIR, "ultimas_operaciones")
MODEL_PATH = os.path.join(BASE_DIR, "ml", "models", "order_model.txt")
META_PATH = MODEL_PATH + ".json"


def cargar_datos():
    archivos = glob.glob(os.path.join(OPERACIONES_DIR, "*.parquet"))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos en {OPERACIONES_DIR}")
    dfs = []
    for archivo in archivos:
        df = pd.read_parquet(archivo)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df["label"] = (df["retorno_total"] > 0).astype(int)

    estrategias = df["estrategias_activas"].apply(lambda x: json.loads(x.replace("'", '"')) if isinstance(x, str) else x)
    todas = sorted({k for d in estrategias for k in d.keys()})
    for col in todas:
        df[col] = estrategias.apply(lambda d: int(d.get(col, False)))
    X = df[todas]
    y = df["label"]
    return X, y, todas


def entrenar():
    if lgb is None:
        raise RuntimeError("LightGBM no está instalado")
    X, y, cols = cargar_datos()
    dtrain = lgb.Dataset(X, label=y)
    params = {"objective": "binary", "verbose": -1}
    model = lgb.train(params, dtrain, num_boost_round=100)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    with open(META_PATH, "w") as f:
        json.dump({"features": cols}, f)
    print(f"✅ Modelo guardado en {MODEL_PATH}")


if __name__ == "__main__":
    entrenar()