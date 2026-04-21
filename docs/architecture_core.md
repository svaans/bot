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

- **`OrderManager`** (`order_manager.py`): orquesta creación/cierre, sincronización con exchange, políticas de mercado/límite, colas de persistencia y métricas. Helpers puros en `order_manager_helpers.py` (balance async, slippage simulado, lados buy/sell). El bucle de reconciliación en background (`run_sync_once` / `run_sync_loop` / backoff de intervalos) vive en `order_manager_sync.py` y recibe la instancia del manager para mutar `ordenes`, métricas y estado de sync sin duplicar la API pública. La tarea periódica **`reconciliar_trades_binance`** (detección de divergencias trades vs estado local) vive en `order_manager_reconcile_tasks.reconcile_trades_loop(manager)` con el reporter `log_reconcile_report_divergences`. La rama **límite vs mercado** (`ejecutar_orden_limit` o `MarketRetryExecutor`) está en `order_manager_execution.execute_real_order`. El flujo largo de **apertura** (`abrir_async`: validación SL/TP, lock anti-duplicados, sync Binance, balance, ejecución, registro SQLite) vive en `order_manager_abrir.abrir_async(manager, …)`; `OrderManager.abrir_async` solo delega pasando `self`. La fachada **`crear`** (normalización de `meta` → cantidad vía `QuantityResolver` → `abrir_async`) vive en `order_manager_crear.crear(manager, …)`. El flujo de **cierre / agregado parcial** (`agregar_parcial_async`, `cerrar_async`, `cerrar_parcial_async`) y los **reencolados** de cierre viven en `order_manager_cerrar` (funciones que reciben `manager`); `OrderManager` reexpone con delegación fina y `_requeue_*` delegan en `requeue_partial_close` / `requeue_full_close`. Los **reintentos de persistencia** tras fallo de `registrar_orden` viven en `order_manager_registro_retry` (`should_schedule_persistence_retry`, `schedule_registro_retry`); el estado `_registro_retry_*` permanece en el manager y `_should_schedule_persistence_retry` / `_schedule_registro_retry` delegan allí.
- **`real_orders.py`**: fachada estable y estado global (caché de órdenes, ventas fallidas, `notificador`, `flush_periodico`). Reexporta helpers de **`real_orders_parse`** (`_coerce_open_orders`, `_extraer_*`, `_coincide_operation_id`) y **`obtener_cliente` / `get_symbol_filters` / `guardar_orden_real`** para que los tests puedan seguir parcheando en `core.orders.real_orders`. CRUD SQLite + caché: **`real_orders_sqlite`** (`load_open_orders`, `persist_orders`, …) y delegados `_connect_db` / `_init_db` / `_validar_datos_orden`.
- **`real_orders_operaciones`**: buffer de operaciones, flush por lotes y Parquet (`persist_operaciones_batch`, `flush_buffer_operaciones`, `registrar_operacion_en_buffer`).
- **`real_orders_metrics`**: `METRICAS_OPERACION` y `acumular_metricas` (fee/PnL por `operation_id`); enlazado en `real_orders` como `_METRICAS_OPERACION` / `_acumular_metricas`.
- **`real_orders_audit`**: `auditar_operacion_post_error` (órdenes cerradas + `consultar_ordenes_abiertas` vía `real_orders` para monkeypatch).
- **`real_orders_reconcile`**: `reconciliar_ordenes`, `sincronizar_ordenes_binance`, `consultar_ordenes_abiertas`, `reconciliar_trades_binance` (caché de open orders); en trades usa `_ro.obtener_cliente` / `_ro.get_symbol_filters` / `_ro.guardar_orden_real` para alinear con parches en `real_orders`.
- **`real_orders_execution`**: `ejecutar_orden_market` / `ejecutar_orden_market_sell` / `_market_sell_retry` / `ejecutar_orden_limit`; resuelve `notificador` y registro vía `import core.orders.real_orders as ro` para evitar ciclos; `_market_sell_retry` y el fallback límite→mercado llaman `ro.ejecutar_orden_market*` para que los parches en `real_orders` apliquen.
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

- Extraer CRUD local (`actualizar_orden` / `eliminar_orden` / `registrar_orden`) a un módulo SQLite delgado si se desea reducir aún más `real_orders.py`.
- Valorar encapsular estado global de `real_orders` en una clase/fábrica para tests paralelos en proceso.

## Auditoría `OrderManager` (estado y siguiente bloque)

- **Hecho (documentado arriba):** `order_manager_abrir`, `order_manager_crear`, `order_manager_cerrar`, `order_manager_registro_retry` (`should_schedule_persistence_retry`, `schedule_registro_retry`; estado `_registro_retry_*` sigue en `OrderManager`), `order_manager_sync`, `order_manager_execution`, `order_manager_reconcile_tasks`.
- **Siguiente bloque razonable:** handlers de bus — `_on_abrir`, `_on_cerrar`, `_on_cerrar_parcial` (delgados) u homogeneizar eventos de órdenes en un solo módulo si crece el catálogo.
