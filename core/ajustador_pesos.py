import json
import os
import numpy as np

def ajustar_pesos_por_desempeno(resultados_backtest, ruta_salida):
    """
    Ajusta y normaliza los pesos de estrategias en función del rendimiento observado.
    Aplica softmax para asignar pesos más diferenciados proporcionalmente.
    """
    pesos_ajustados = {}

    for symbol, resultados in resultados_backtest.items():
        # Filtrar solo valores numéricos positivos
        valores_validos = {
            k: v for k, v in resultados.items()
            if isinstance(v, (int, float)) and v >= 0
        }

        if not valores_validos:
            print(f"⚠️ Sin datos válidos para {symbol}. Saltando...")
            continue

        valores = np.array(list(valores_validos.values()))
        exp_vals = np.exp(valores)
        suma_exp = exp_vals.sum()

        pesos_normalizados = {
            estrategia: round((np.exp(valor) / suma_exp) * 10, 2)
            for estrategia, valor in valores_validos.items()
        }

        pesos_ajustados[symbol] = pesos_normalizados

    # Guardar resultado
    try:
        with open(ruta_salida, "w") as f:
            json.dump(pesos_ajustados, f, indent=4)
        print(f"✅ Pesos ajustados guardados en {ruta_salida}")
    except OSError as e:
        print(f"❌ Error al guardar pesos en {ruta_salida}: {e}")
        raise

    return pesos_ajustados

