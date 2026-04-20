# Arquitectura de `core/` (visión de conjunto)

Este documento resume cómo encajan las piezas principales del núcleo del bot. La intención es orientar refactors y onboarding sin duplicar reglas de negocio línea a línea.

## Pipeline de velas (`core/vela/`)

- **`core.procesar_vela`**: fachada estable; reexporta símbolos desde `core.vela.pipeline` para que el resto del repo no dependa de rutas internas.
- **`core.vela.pipeline`**: módulo de **API estable** del subsistema (métricas, buffers, circuit breaker, `procesar_vela`, etc.). Los tests suelen parchear atributos en `core.vela.pipeline` (no en la fachada) cuando afectan al código que ejecuta el handler.
- **`core.vela.pipeline_procesar`**: implementación del handler asíncrono `procesar_vela` y la fase previa de salidas (`_maybe_verificar_salidas_vela`). Al inicio de cada invocación resuelve las métricas vía `import core.vela.pipeline as _pm` y usa `_pm.CANDLES_IGNORADAS`, etc., para que los tests que parchean `core.vela.pipeline` sigan viendo el mismo punto de extensión.
- **`core.vela.metrics_definitions`**: histogramas/counters y gauges importados o definidos aquí; comparten instancia con lo reexportado en `pipeline`.
- **`core.vela.buffers`**: `BufferManager` / estado por símbolo. La métrica `BUFFER_SIZE_V2` se resuelve con import diferido a `core.vela.pipeline` para respetar monkeypatch en tests.
- **`core.vela.helpers`**, **`spread`**, **`circuit_breaker`**, **`order_open`**: utilidades y subsistemas acotados.

Flujo lógico de una vela cerrada: validación → spread guard → buffer/DataFrame → indicadores incrementales / warmup → salidas si hay posición → fastpath opcional → evaluación de entrada → apertura con reintentos/circuit breaker → métricas de latencia en `finally`.

## Órdenes (`core/orders/`)

- **`OrderManager`** (`order_manager.py`): orquesta creación/cierre, sincronización con exchange, políticas de mercado/límite, colas de persistencia y métricas. Helpers puros en `order_manager_helpers.py` (balance async, slippage simulado, lados buy/sell). El bucle de reconciliación en background (`run_sync_once` / `run_sync_loop` / backoff de intervalos) vive en `order_manager_sync.py` y recibe la instancia del manager para mutar `ordenes`, métricas y estado de sync sin duplicar la API pública. La rama **límite vs mercado** (`ejecutar_orden_limit` o `MarketRetryExecutor`) está en `order_manager_execution.execute_real_order`. El flujo largo de **apertura** (`abrir_async`: validación SL/TP, lock anti-duplicados, sync Binance, balance, ejecución, registro SQLite) vive en `order_manager_abrir.abrir_async(manager, …)`; `OrderManager.abrir_async` solo delega pasando `self`.
- **`real_orders.py`**: persistencia SQLite, buffers de operaciones, reconciliación con CCXT y utilidades de órdenes reales. Mantiene estado global documentado en el docstring del módulo; convivencia multi-instancia en un mismo proceso requiere diseño explícito. Helpers sin estado del propio módulo se reexportan desde **`real_orders_async`** (puente sync/async) y **`real_orders_parse`** (lectura de dicts estilo CCXT / `operation_id`).
- **`orders.py`**, **`order_model.py`**, **`order_helpers.py`**, **`quantity_resolver.py`**, **`market_retry_executor.py`**: piezas de soporte.

## Gestor de órdenes de alto nivel (`core/gestor_ordenes.py`)

- **`GestorOrdenes`**: capa reusable con **inyección explícita** de bus, `orders`, reporter, auditor, etc. No es el camino principal del `StartupManager` del bot en vivo (ahí suele usarse `OrderManager` vía `PositionManager`), pero sirve para pruebas e integraciones que quieren cablear dependencias a mano.

## Trader y modo operativo

- **`core.trader`**: `Trader` / `TraderLite` consumen el pipeline de velas (`procesar_vela`), buffers y órdenes según configuración.
- **`core.operational_mode`**: modo operativo (p. ej. paper vs real) desacoplado donde aplique.

## Convenciones para tests

- Parches de métricas del pipeline: preferir **`core.vela.pipeline`** (objetos compartidos con el handler) salvo que el símbolo solo exista en un submódulo.
- Parches de helpers puros: el módulo donde viva la función (`core.vela.helpers`, etc.).

## Trabajo pendiente (evolución)

- Seguir troceando `order_manager.py` y `real_orders.py` por dominios (sync, crear/cerrar, persistencia) sin romper la API pública de `core.orders`.
- Valorar encapsular estado global de `real_orders` en una clase/fábrica para tests paralelos en proceso.
