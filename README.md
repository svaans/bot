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
