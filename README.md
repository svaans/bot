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

Las órdenes abiertas ahora se guardan en una base SQLite ubicada en `ordenes_reales/ordenes.db`. Si el proceso se reinicia, las órdenes se cargan automáticamente desde esa base de datos para continuar su seguimiento.

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
- **StrategyEngine** evalúa las estrategias de entrada y salida.
- **RiskManager** centraliza las comprobaciones de riesgo diario.
- **OrderManager** registra y ejecuta las operaciones reales.

La clase `Trader` orquesta estos módulos y mantiene compatibilidad con
`TraderSimulado` para los escenarios de backtesting.

## Logging estructurado y reportes

En modo real el bot utiliza un logger en formato JSON cuya salida se guarda en
`logs/bot.log`. Cada operación cerrada se agrega al directorio
`reportes_diarios/` y al finalizar el día se genera automáticamente un PDF con
las métricas principales: ganancia acumulada, winrate y drawdown.


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
## Pesos técnicos y umbrales adaptativos

Los puntajes técnicos se calculan a partir de varios criterios cuyo peso se
define en `core/strategies/evaluador_tecnico.py`. Para personalizarlos puede
crearse el archivo `config/pesos_tecnicos.json` y ajustar allí cada valor.

Los módulos `core/adaptador_dinamico.py` y `core/adaptador_umbral.py`
adaptan automáticamente los umbrales tomando en cuenta la volatilidad,
la pendiente del precio (slope) y el RSI actual. Estos factores y sus
multiplicadores base pueden modificarse en
`config/configuraciones_optimas.json`.
