# Trading Bot

Este proyecto implementa un bot de trading para Binance.

## Variables de entorno

El bot obtiene las claves y parámetros desde el entorno del sistema. Antes de ejecutarlo asegúrate de definir las siguientes variables:

- `BINANCE_API_KEY` – clave de API de Binance.
- `BINANCE_API_SECRET` – secreto de la API de Binance.
- `MODO_REAL` – `True` para operar en real o `False` para simular (opcional, por defecto `False`).
- `UMBRAL_RIESGO_DIARIO` – porcentaje máximo de riesgo diario (opcional, por defecto `0.03`).

Estas variables pueden definirse en tu shell o mediante un archivo `.env` que cargues manualmente antes de iniciar el bot.

## Persistencia de órdenes

El bot guarda las operaciones abiertas en `ordenes_reales/ordenes_reales.parquet`. Si el proceso se reinicia, las órdenes se cargan automáticamente para continuar su seguimiento y evitar pérdidas de información.