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
   cd fronted
   npm install
   ```

## Persistencia de órdenes

El bot guarda las operaciones abiertas en `ordenes_reales/ordenes_reales.parquet`. Si el proceso se reinicia, las órdenes se cargan automáticamente para continuar su seguimiento y evitar pérdidas de información.

## Pruebas

Para ejecutar las pruebas del backend (Django) usa:

```bash
python backend/manage.py test
```

Las pruebas del frontend se ejecutan con:

```bash
npm test --prefix fronted

## Arquitectura modular

Las funcionalidades principales del bot están divididas en componentes dentro de
`core/`:

- **DataFeed** se encarga del _stream_ de velas (`core/data_feed.py`).
- **StrategyEngine** evalúa las estrategias de entrada y salida.
- **RiskManager** centraliza las comprobaciones de riesgo diario.
- **OrderManager** registra y ejecuta las operaciones reales.

La clase `Trader` orquesta estos módulos y mantiene compatibilidad con
`TraderSimulado` para los escenarios de backtesting.


## Instalación

Instala todas las dependencias del proyecto y del backend ejecutando el
siguiente comando desde la raíz del repositorio:

```bash
pip install -r requirements.txt -r backend/requirements.txt
```

Con esto tendrás todas las dependencias necesarias para ejecutar el bot.