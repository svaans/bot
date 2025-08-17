# Flujo de salida

El proceso de salida sigue las etapas:

```
señal → trigger → decisión → construcción → envío → confirmación → reconciliación → notificación
```

## Etapas y riesgos

1. **Señal**
   - **Bloqueos:** latencia de red al recibir señales.
   - **Duplicidad:** señales repetidas sin `operation_id`.
   - **Pérdida de estado:** la señal se descarta antes de activar el *trigger*.

2. **Trigger**
   - **Bloqueos:** espera de eventos o *locks* en la cola.
   - **Duplicidad:** reintentos que no incluyen `operation_id`.
   - **Pérdida de estado:** el disparo se pierde sin registrar la activación.

3. **Decisión**
   - **Bloqueos:** acceso a bases de datos o recursos compartidos.
   - **Duplicidad:** evaluaciones múltiples de la misma operación.
   - **Pérdida de estado:** fallo antes de persistir la decisión tomada.

4. **Construcción**
   - **Bloqueos:** dependencia de servicios externos o construcción de payload.
   - **Duplicidad:** órdenes generadas repetidamente sin `operation_id`.
   - **Pérdida de estado:** la orden se prepara pero no llega a enviarse.

5. **Envío**
   - **Bloqueos:** latencia de red, *timeouts*.
   - **Duplicidad:** reenvío sin `operation_id` crea órdenes duplicadas.
   - **Pérdida de estado:** desconexión entre envío y confirmación.

6. **Confirmación**
   - **Bloqueos:** espera de la respuesta del exchange.
   - **Duplicidad:** confirmaciones duplicadas o *callbacks* repetidos.
   - **Pérdida de estado:** no se recibe confirmación y el estado queda incierto.

7. **Reconciliación**
   - **Bloqueos:** *locks* en la base de datos durante la conciliación.
   - **Duplicidad:** procesos paralelos sin coordinación de `operation_id`.
   - **Pérdida de estado:** inconsistencia entre registros locales y remotos.

8. **Notificación**
   - **Bloqueos:** canales de mensajería saturados.
   - **Duplicidad:** múltiples notificaciones por la misma operación.
   - **Pérdida de estado:** los usuarios no reciben confirmación final.

## Mantenimiento

Actualiza este documento cuando se agreguen nuevos pasos, métricas o consideraciones al flujo de salida.