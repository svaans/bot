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

## Servicio de velas en Go

El bot consume las velas de Binance a través de un servicio externo escrito en Go.
Para compilarlo manualmente:

```bash
cd candle_service
./build.sh
./candle-service
```

También puedes ejecutarlo con Docker:

```bash
docker build -t candle_service ./candle_service
docker run -p 9000:9000 candle_service
```

`main.py` inicia automáticamente este servicio cuando se ejecuta el bot. Si
prefieres levantarlo dentro de un contenedor usa la variable de entorno
`USE_DOCKER_CANDLE=1`. Para desactivar este comportamiento establece
`NO_AUTO_CANDLE=1`.

El bot Python asume que el servicio está disponible en `localhost:9000`.
Puedes cambiar esta dirección con las variables de entorno `CANDLE_HOST` y
`CANDLE_PORT`.

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

Ejemplos de mensajes:

```
[BTC/USDT] persistencia_detectada | repetidas=2.00, minimo=1.50
[ETH/USDT] score_tecnico | score=3.20, rsi=55.30, momentum=0.12, slope=0.45, tendencia=alcista
```

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

## Ejecución completa en producción

Para iniciar el bot junto a sus microservicios se incluye el script
`scripts/supervisor.py`. Este script lanza el servicio de velas y el worker de
persistencia antes de ejecutar `main.py`:

```bash
python scripts/supervisor.py
```

Las direcciones de cada servicio pueden ajustarse con las variables de entorno
`CANDLE_HOST`/`CANDLE_PORT`, `WS_SERVICE_HOST`/`WS_SERVICE_PORT` y
`ORDERS_WORKER_HOST`/`ORDERS_WORKER_PORT`.

Si prefieres usar contenedores puedes levantar todo con Docker Compose:

```bash
El archivo `docker-compose.yml` crea los servicios `candle-service`,
`orders-worker` y `bot`. El contenedor del bot ejecuta automáticamente
`scripts/supervisor.py` para iniciar todos los microservicios.
## Ajuste de pesos y umbrales

La ponderación que recibe cada indicador técnico se encuentra en
`core/config/pesos.py` bajo la constante `PESOS_SCORE_TECNICO`. Para modificar
el umbral mínimo requerido por el score técnico usa la variable de entorno
`UMBRAL_SCORE_TECNICO` o edita los valores por defecto en
`config/development.py` o `config/production.py`.

## Extensión acelerada con Rust

Para obtener mejores tiempos de cálculo en el ajuste de TP/SL se incluye una extensión opcional escrita en Rust usando **PyO3**.

### Compilación

1. Instala el toolchain de Rust desde [rustup.rs](https://rustup.rs/).
2. Instala `maturin`:
   ```bash
   pip install maturin
   ```
3. Compila la extensión desde la raíz del proyecto:
   ```bash
   maturin develop --release -m fast_tp_sl/Cargo.toml
   ```

Si la extensión se encuentra disponible, `core.adaptador_dinamico` la cargará automáticamente. En caso contrario se utilizará la implementación en Python.
