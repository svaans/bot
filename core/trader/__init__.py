"""
Paquete core.trader
Expone la clase Trader orquestadora y sus funciones modulares
"""

from .trader import Trader
from .entrada import (
    evaluar_condiciones_entrada,
    evaluar_condiciones_de_entrada,
    abrir_operacion_real
)
from .salida import (
    cerrar_operacion,
    cerrar_y_reportar,
    cerrar_parcial_y_reportar,
    verificar_salidas
)
from .capital import ajustar_capital_diario
from .gestion_estado import (
    guardar_estado_persistente,
    cargar_estado_persistente
)
from .persistencia import (
    evaluar_persistencia,
    tendencia_persistente,
    validar_reentrada_tendencia
)
from .puntuacion import (
    calcular_score_tecnico_trader,
    validar_puntaje,
    registrar_rechazo_tecnico,
    hay_contradicciones,
    validar_temporalidad
)
from .tareas import (
    ciclo_aprendizaje_periodico,
    precargar_historico
)
