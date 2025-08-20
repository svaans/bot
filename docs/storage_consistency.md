# Consistencia y almacenamiento de órdenes

Este módulo persiste órdenes en SQLite y presenta tres riesgos principales:

1. **Escritura parcial**: insertar múltiples registros sin transacción podía
   dejar datos a medias si una operación fallaba. El uso de un bloque
   transaccional asegura que todas las órdenes se escriban o ninguna lo haga.
2. **Falta de validación de entrada**: registros con campos vacíos o de tipo
   incorrecto generaban errores en tiempo de ejecución. Se añadieron validaciones
   mínimas (`symbol`, `precio_entrada`, `cantidad`) antes de escribir.
3. **_Schema drift_**: cambios en la estructura del modelo pueden no reflejarse
   en la base existente. Se recomienda gestionar migraciones o verificar
   periódicamente la correspondencia de columnas para evitar inconsistencias.