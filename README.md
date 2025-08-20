# Trading Bot

Este proyecto implementa un bot de trading para Binance.

## Instalación

1. Instala las dependencias de Python:
   ```bash
   pip install -r requirements.txt
   pip install -r backend/requirements.txt
   ```
2. Instala las dependencias de Node para la interfaz web:
   ```bash
   cd frontend
   npm install
   ```

## Persistencia de órdenes

Las órdenes abiertas se guardan en una base SQLite cuya ruta puede ajustarse mediante la variable de entorno `ORDENES_DB_PATH` (por defecto `ordenes_reales/ordenes.db`). Si el proceso se reinicia, las órdenes se cargan automáticamente desde esa base de datos para continuar su seguimiento. Los CSV de resultados se escriben en el directorio definido por `ORDENES_DIR` (`ordenes_reales` si no se especifica).

El histórico de velas se busca en el directorio indicado por `DATOS_DIR` (predeterminado `datos`) y el estado persistente del bot se guarda en `ESTADO_DIR` (predeterminado `estado`).

Para migrar un archivo `.parquet` existente ejecuta:

```bash
python scripts/migrate_parquet_to_db.py
```

## Pruebas

Para ejecutar las pruebas del backend (Django) usa:

```bash
python backend/manage.py test
```

Las pruebas del frontend se ejecutan con:

```bash
npm test --prefix frontend
```
Para probar los componentes del bot con **pytest**:

```bash
pytest
```

En CI se recomienda ejecutar estas pruebas en este orden para asegurar la integridad del proyecto.

## Arquitectura modular

Las funcionalidades principales del bot están divididas en componentes dentro de
`core/`:

- **DataFeed** se encarga del _stream_ de velas (`core/data/data_feed.py`).
  El WebSocket envía un *ping* de keepalive cada 60&nbsp;segundos para detectar
  desconexiones de forma temprana. También puede activarse **TCP keep-alive** a
  nivel de sistema para mantener la conexión incluso sin tráfico de
  aplicación. Verifica las reglas de firewall o NAT de tu proveedor, ya que
  algunas plataformas cierran WebSockets inactivos tras unos minutos.
  El intervalo de vigilancia se controla con `MONITOR_INTERVAL` (5 s por defecto)
  y los streams se reinician tras `INACTIVITY_INTERVALS` intervalos sin datos
  (valor predeterminado 3, sin mínimo fijo de 300 s).
- **StrategyEngine** evalúa las estrategias de entrada y salida.
- **RiskManager** centraliza las comprobaciones de riesgo diario.
- **OrderManager** registra y ejecuta las operaciones reales.

La clase `Trader` orquesta estos módulos y mantiene compatibilidad con
`TraderSimulado` para los escenarios de backtesting.

## Streams por símbolo

El bot utiliza un WebSocket independiente para cada par, lo que permite
reiniciar únicamente el flujo afectado si se detecta inactividad o una
desconexión. De esta forma, los demás símbolos continúan recibiendo
datos sin interrupciones.

## Logging estructurado y reportes

En modo real el bot utiliza un logger en formato JSON cuya salida se guarda en
`$LOG_DIR/bot.log` (por defecto `logs/bot.log`). A partir de esta versión el
archivo rota de manera diaria y se conservan varias copias (configurable desde
`configurar_logger`). Cada operación cerrada se agrega al directorio
`reportes_diarios/` y al finalizar el día se genera automáticamente un PDF con
las métricas principales: ganancia acumulada, winrate y drawdown.
Además, cada `frecuencia_recursos` segundos `procesar_vela` registra el uso de
CPU y memoria para facilitar la detección de cuellos de botella.
El intervalo se controla mediante el parámetro `FRECUENCIA_RECURSOS` (60s por
defecto).

## Supervisión proactiva

- Se envía una alerta si el uso de CPU o memoria supera los umbrales
  `UMBRAL_ALERTA_CPU` o `UMBRAL_ALERTA_MEM` durante `CICLOS_ALERTA_RECURSOS`
  ciclos consecutivos.
- Al comenzar un nuevo día, si se había alcanzado el límite de riesgo diario,
  el bot notifica que el riesgo se restableció y que las operaciones se
  reanudan.


## Instalación

Instala todas las dependencias del proyecto y del backend ejecutando el
siguiente comando desde la raíz del repositorio:

```bash
pip install -r requirements.txt -r backend/requirements.txt
```

Después de instalar los paquetes, el bot quedará listo para ejecutarse.

## Configuración por entorno

El gestor de configuración carga variables desde `config/claves.env` y puede
ajustar valores por defecto según el entorno. Usa la variable de entorno
`BOT_ENV` para elegir entre `development` o `production`. Por ejemplo:

```bash
BOT_ENV=production python main.py
```

Los valores base de cada modo se definen en `config/development.py` y
`config/production.py`.

## Parámetros de salida

En `config/exit_defaults.py` se definen los valores iniciales para todas las estrategias de cierre. Estos son aplicados automáticamente con `load_exit_config()` y validados por `validate_exit_config()`.

- `factor_umbral_sl`: Factor aplicado al umbral técnico para permitir evitar un Stop Loss.
- `min_estrategias_relevantes_sl`: Número mínimo de estrategias alineadas para considerar válido un SL.
- `sl_ratio`: Relación ATR usada para ajustar dinámicamente el nivel de SL.
- `max_velas_sin_tp`: Cantidad máxima de velas antes de forzar cierre si no se alcanzó el TP.
- `max_evitar_sl`: Intentos permitidos para evitar el SL por criterios técnicos.
- `atr_multiplicador`: Multiplicador de ATR en cálculos de trailing y break-even.
- `trailing_pct`: Porcentaje mínimo de distancia para el trailing stop.
- `trailing_buffer`: Margen adicional para actualizar el máximo o mínimo antes de activar el trailing.
- `trailing_start_ratio`: Relación de precio respecto a la entrada para comenzar el trailing.
- `trailing_distance_ratio`: Distancia relativa al máximo/mínimo para ejecutar el trailing.
- `trailing_por_atr`: Si se usa el ATR en lugar de porcentajes para el trailing.
- `uso_trailing_technico`: Ajusta el trailing stop utilizando soportes y resistencias recientes.
- `tp_ratio`: Multiplicador aplicado al ATR para calcular un Take Profit técnico.
- `volumen_minimo_salida`: Volumen mínimo exigido para validar una salida inteligente.
- `max_spread_ratio`: Spread máximo permitido antes de posponer un cierre.
- `periodo_atr`: Período para el cálculo de ATR en Break Even.
- `break_even_atr_mult`: Multiplicador ATR para definir el umbral de Break Even.
- `umbral_rsi_salida`: Nivel de RSI sobre el cual se evita cerrar una posición long.
- `factor_umbral_validacion_salida`: Factor aplicado al puntaje técnico para validar un cierre.
- `umbral_puntaje_macro_cierre`: Valor absoluto de puntaje macroeconómico que fuerza la salida inmediata.
- `max_intentos_cierre`: Número de reintentos permitidos al cerrar por spread alto.
- `delay_reintento_cierre`: Espera en segundos entre reintentos de cierre cuando el spread es elevado.
- `max_velas_reversion_tendencia`: Cantidad de velas consecutivas que señalan una reversión de tendencia antes de forzar el cierre de la posición (por defecto 3).

## Límites de timeout

Los parámetros `timeout_verificar_salidas`, `timeout_evaluar_condiciones` y `timeout_bus_eventos` determinan los segundos máximos de espera al validar una salida, evaluar las condiciones de entrada o recibir confirmaciones del bus interno. Se recomienda mantenerlos en **20 s**, **15 s** y **10 s** respectivamente para no demorar reacciones ni sobrecargar el sistema.

El parámetro `handler_timeout` controla cuánto puede tardar el procesamiento de una vela antes de descartarla (por defecto **5 s**). Ajustarlo según el hardware ayuda a evitar falsos positivos de timeout.

Para reducir el costo de cálculo, `frecuencia_tendencia` permite recalcular la tendencia del mercado cada *N* velas en lugar de hacerlo en todas. Un valor de **3** suele equilibrar bien precisión y rendimiento.


El parámetro `max_timeouts_salidas` controla cuántos timeouts consecutivos al verificar una salida se toleran antes de forzar un cierre de emergencia (por defecto **3**).

## Pesos técnicos y umbrales adaptativos

Los puntajes técnicos se calculan a partir de varios criterios cuyo peso se
define en `core/strategies/evaluador_tecnico.py`. Para personalizarlos puede
crearse el archivo `config/pesos_tecnicos.json` y ajustar allí cada valor.

Los módulos `core/adaptador_dinamico.py` y `core/adaptador_umbral.py`
adaptan automáticamente los umbrales tomando en cuenta la volatilidad,
la pendiente del precio (slope) y el RSI actual. Estos factores y sus
multiplicadores base pueden modificarse en
`config/configuraciones_optimas.json`.

## Validación continua de la estrategia

Para mantener el desempeño del bot se recomienda revisar periódicamente
las reglas y parámetros aplicados:

- **Backtesting intensivo**: utiliza `TraderSimulado` para probar cambios
  de lógica o parámetros con datos históricos antes de desplegarlos en
  real.
- **Análisis de métricas**: los filtros aplicados se registran mediante
  `metricas_tracker`. Revisa estos datos para detectar si algún filtro
  como "sin estrategias", "umbral" o "diversidad" está descartando
  operaciones de forma excesiva.
- **Impacto de Stop Loss evitados**: el archivo
  `logs/impacto_sl.log` registra cada vez que se evita un SL por razones
  técnicas. Analizar cuántos de estos casos terminan en pérdidas mayores
  ayuda a ajustar la lógica de evasión.
- **Simulación de distintos escenarios**: prueba la estrategia en
  tendencias fuertes, rangos laterales y periodos de alta volatilidad para
  confirmar que los filtros de tendencia y persistencia responden
  correctamente.
- **Pruebas unitarias ampliadas**: extiende la suite existente con casos
  borde para las funciones de cálculo técnico clave.
- **Modo simulación en tiempo real**: antes de aplicar cambios en
  producción, ejecuta el bot en papel unos días y compara los resultados
  con lo esperado.
- **Recalibración de parámetros**: ajusta regularmente los valores de
  `config/exit_defaults.py` (`factor_umbral_sl`, `trailing_pct`,
  `sl_ratio`, etc.) para que reflejen las condiciones actuales del
  mercado.
