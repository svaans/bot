import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import numpy as np
from scipy.optimize import minimize

RUTA_CONFIG = Path('config/configuraciones_optimas.json')


def _cargar_config(path: Path) -> dict:
    if path.exists():
        with open(path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    return {}


def _guardar_config(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, indent=2)


@dataclass(frozen=True)
class AdaptiveWeightsCalibration:
    """Resultado de la calibración de pesos adaptativos."""

    weights: dict[str, float]
    train_mse: float
    validation_mse: float
    n_train: int
    n_validation: int
    regularization: float


def _objetivo(pesos: np.ndarray, df: pd.DataFrame, l2_penalty: float) -> float:
    pred = (
        df['volatilidad'] * pesos[0]
        + df['rango'] * pesos[1]
        + df['volumen'] * pesos[2]
        + df['momentum'] * pesos[3]
    )
    mse = float(np.mean((pred - df['objetivo']) ** 2))
    if l2_penalty:
        mse += float(l2_penalty) * float(np.sum(pesos ** 2))
    return mse


def _mse(pesos: np.ndarray, df: pd.DataFrame) -> float:
    pred = (
        df['volatilidad'] * pesos[0]
        + df['rango'] * pesos[1]
        + df['volumen'] * pesos[2]
        + df['momentum'] * pesos[3]
    )
    return float(np.mean((pred - df['objetivo']) ** 2))


def calibrar_pesos(
    path_datos: Path,
    path_config: Path = RUTA_CONFIG,
    *,
    validation_fraction: float = 0.25,
    l2_penalty: float = 1e-3,
) -> AdaptiveWeightsCalibration:
    df = pd.read_csv(path_datos)
    if not 0 < validation_fraction < 1:
        raise ValueError('validation_fraction debe estar en (0, 1)')
    if {'volatilidad', 'rango', 'volumen', 'momentum', 'objetivo'} - set(df.columns):
        raise ValueError(
            "El dataset debe contener las columnas volatilidad,rango,volumen,momentum,objetivo"
        )
    total_registros = len(df)
    if total_registros < 10:
        raise ValueError('Se requieren al menos 10 registros para calibrar pesos')
    val_min_size = max(1, int(total_registros * validation_fraction))
    if val_min_size >= total_registros:
        raise ValueError('validation_fraction produce un conjunto de validación vacío')
    val_size = max(val_min_size, 5)
    if total_registros - val_size < 4:
        raise ValueError('El conjunto de entrenamiento queda demasiado pequeño para calibrar')
    train_df = df.iloc[: total_registros - val_size].copy()
    val_df = df.iloc[total_registros - val_size :].copy()
    x0 = np.array([0.3, 0.3, 0.2, 0.2])
    bounds = [(0, 1)] * 4
    cons = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
    res = minimize(
        _objetivo,
        x0,
        args=(train_df, float(l2_penalty)),
        bounds=bounds,
        constraints=cons,
    )
    if not res.success:
        raise RuntimeError(f"Optimización fallida: {res.message}")
    pesos = {
        'volatilidad': round(res.x[0], 4),
        'rango': round(res.x[1], 4),
        'volumen': round(res.x[2], 4),
        'momentum': round(res.x[3], 4),
    }
    train_mse = _mse(res.x, train_df)
    val_mse = _mse(res.x, val_df)
    resultado = AdaptiveWeightsCalibration(
        weights=pesos,
        train_mse=train_mse,
        validation_mse=val_mse,
        n_train=len(train_df),
        n_validation=len(val_df),
        regularization=float(l2_penalty),
    )
    cfg = _cargar_config(path_config)
    cfg['pesos_contexto'] = pesos
    cfg['metricas_pesos_contexto'] = {
        'train_mse': train_mse,
        'validation_mse': val_mse,
        'n_train': resultado.n_train,
        'n_validation': resultado.n_validation,
        'validation_fraction': validation_fraction,
        'regularization': resultado.regularization,
    }
    _guardar_config(cfg, path_config)
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Calibra pesos adaptativos usando datos históricos.'
    )
    parser.add_argument(
        'dataset',
        help='CSV con columnas volatilidad,rango,volumen,momentum,objetivo',
    )
    parser.add_argument(
        '--salida',
        default=str(RUTA_CONFIG),
        help='Ruta del archivo de configuración de salida',
    )
    args = parser.parse_args()
    resultado = calibrar_pesos(Path(args.dataset), Path(args.salida))
    print('Pesos optimizados:', resultado.weights)
    print(
        'MSE entrenamiento:',
        f"{resultado.train_mse:.6f}",
        '| MSE validación:',
        f"{resultado.validation_mse:.6f}",
    )


if __name__ == '__main__':
    main()
