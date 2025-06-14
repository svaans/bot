import json
import os
import numpy as np
from core.logger import configurar_logger

log = configurar_logger("ajustador_pesos")


def ajustar_pesos_por_desempeno(resultados_backtest: dict, ruta_salida: str) -> dict:
    """
    Ajusta y normaliza los pesos de estrategias por símbolo usando softmax estable.
    Guarda un JSON con los pesos escalados en una escala de 0 a 10.
    """
    pesos_ajustados = {}

    for symbol, resultados in resultados_backtest.items():
        # Filtrar estrategias válidas (números positivos)
        valores_validos = {
            estrategia: v for estrategia, v in resultados.items()
            if isinstance(v, (int, float)) and v >= 0
        }

        if not valores_validos:
            log.warning(f"⚠️ Sin datos válidos para {symbol}. Saltando...")
            continue

        estrategias = list(valores_validos.keys())
        valores = np.array(list(valores_validos.values()), dtype=np.float64)

        # Softmax estable (evita overflow exponencial)
        valores_stable = valores - np.max(valores)
        exp_vals = np.exp(valores_stable)
        suma_exp = exp_vals.sum()

        if suma_exp == 0:
            log.warning(f"⚠️ Softmax colapsó para {symbol}. Asignando pesos iguales.")
            pesos_normalizados = {k: round(10.0 / len(valores_validos), 2) for k in estrategias}
        else:
            pesos_normalizados = {
                estrategia: round((np.exp(v - np.max(valores)) / suma_exp) * 10, 2)
                for estrategia, v in valores_validos.items()
            }

        pesos_ajustados[symbol] = pesos_normalizados
        log.info(f"✅ Pesos calculados para {symbol}: {pesos_normalizados}")

    # Guardar resultados
    try:
        with open(ruta_salida, "w") as f:
            json.dump(pesos_ajustados, f, indent=4)
        log.info(f"📁 Pesos ajustados guardados en {ruta_salida}")
    except OSError as e:
        log.error(f"❌ Error al guardar pesos en {ruta_salida}: {e}")
        raise

    return pesos_ajustados


