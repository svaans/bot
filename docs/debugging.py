# Debugging

Los logs del bot se generan en formato JSON con campos como `symbol`, `timeframe`, `timestamp` y `evento`. Esto permite realizar búsquedas precisas.

Por ejemplo, para buscar en el archivo `logs/bot.log` las entradas del símbolo `BTCUSDT` en el timeframe `1m` relacionadas con el evento `procesar_vela`, puedes usar `jq`:

```bash
jq -c 'select(.symbol=="BTCUSDT" and .timeframe=="1m" and .evento=="procesar_vela")' logs/bot.log
```

Este comando mostrará únicamente los registros que coincidan con esos criterios.
