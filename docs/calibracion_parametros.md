# Calibración de parámetros adaptativos

Este proyecto utiliza diversos coeficientes heurísticos para ajustar el
comportamiento del bot de trading. Para mejorar la robustez a largo
plazo es recomendable calibrar estos valores con datos históricos.

## Optimización de pesos de contexto

El script `learning/calibrar_parametros_adaptativos.py` permite ajustar
de forma automática los pesos usados en el cálculo del `contexto_score`
dentro de `core/adaptador_dinamico.py`.

### Uso

1. Prepare un archivo CSV con las columnas:
   `volatilidad`, `rango`, `volumen`, `momentum` y `objetivo`. La columna
   `objetivo` debe representar el valor deseado del puntaje de contexto
durante el backtest.
2. Ejecute el calibrador:

```bash
python learning/calibrar_parametros_adaptativos.py datos.csv
```

3. El script generará/actualizará `config/configuraciones_optimas.json`
   con los pesos optimizados. En posteriores ejecuciones del bot se
   cargarán automáticamente.

Esta calibración no modifica la estructura del sistema y permite afinar
los coeficientes adaptativos para obtener un comportamiento más acorde a
los resultados históricos.
