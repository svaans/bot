# filtros/validador_entradas.py

def evaluar_validez_estrategica(symbol, df, estrategias_activas: dict, pesos=None, minimo_peso_total=0.5, min_diversidad=2) -> bool:
    activas = [k for k, v in estrategias_activas.items() if v]
    if len(activas) < min_diversidad:
        return False

    if pesos:
        peso_total = sum(pesos.get(k, 0) for k in activas)
        if peso_total < minimo_peso_total:
            return False

    return True



