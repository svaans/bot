"""Evita que el estado global del umbral adaptativo contamine comparaciones entre configs."""


def reset_umbral_adaptativo_runtime() -> None:
    """Limpia memoria de suavizado/histéresis usada por :func:`core.adaptador_umbral.calcular_umbral_adaptativo`.

    Marca el estado como ya cargado sin volver a leer ``estado/umbral_adaptativo.json``,
    de modo que cada replay arranca con umbrales suavizados vacíos (solo el histórico
    del propio replay). Llama a esto **una vez al inicio** de cada corrida aislada.
    """

    import core.adaptador_umbral as m

    m._UMBRAL_SUAVIZADO.clear()
    m._HISTERESIS_SKIPS.clear()
    m._UMBRAL_HISTORICO.clear()
    m._UMBRAL_ESTADO_CARGADO = True
