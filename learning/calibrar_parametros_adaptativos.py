import argparse
import json
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


def _objetivo(pesos: np.ndarray, df: pd.DataFrame) -> float:
    pred = (
        df['volatilidad'] * pesos[0]
        + df['rango'] * pesos[1]
        + df['volumen'] * pesos[2]
        + df['momentum'] * pesos[3]
    )
    return float(np.mean((pred - df['objetivo']) ** 2))


def calibrar_pesos(path_datos: Path, path_config: Path = RUTA_CONFIG) -> dict:
    df = pd.read_csv(path_datos)
    x0 = np.array([0.3, 0.3, 0.2, 0.2])
    bounds = [(0, 1)] * 4
    cons = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
    res = minimize(_objetivo, x0, args=(df,), bounds=bounds, constraints=cons)
    if not res.success:
        raise RuntimeError(f"Optimización fallida: {res.message}")
    pesos = {
        'volatilidad': round(res.x[0], 4),
        'rango': round(res.x[1], 4),
        'volumen': round(res.x[2], 4),
        'momentum': round(res.x[3], 4),
    }
    cfg = _cargar_config(path_config)
    cfg['pesos_contexto'] = pesos
    _guardar_config(cfg, path_config)
    return pesos


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
    pesos = calibrar_pesos(Path(args.dataset), Path(args.salida))
    print('Pesos optimizados:', pesos)


if __name__ == '__main__':
    main()
