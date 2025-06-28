# Candle Service

Este microservicio expone un stream de velas de Binance por gRPC.

## Uso rapido

```bash
docker-compose up --build
```

El servicio quedará disponible en `localhost:9000`.

### Ejemplo con grpcurl

```bash
grpcurl -plaintext -d '{"symbol":"BTCUSDT","interval":"1m"}' localhost:9000 candle.CandleService/Subscribe
```