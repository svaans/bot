import numpy as np


def ema_np(arr: np.ndarray, periodo: int) -> np.ndarray:
    """Calcula una EMA tipo Wilder usando solo NumPy."""
    if arr.size == 0:
        return arr
    alpha = 1 / periodo
    ema = np.empty_like(arr, dtype=float)
    ema[0] = arr[0]
    for i in range(1, arr.size):
        ema[i] = alpha * arr[i] + (1 - alpha) * ema[i - 1]
    return ema