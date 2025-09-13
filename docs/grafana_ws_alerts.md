# Panel de reconexiones WebSocket en Grafana

Este panel utiliza las métricas `ws_reconnections_total` y `ws_unexpected_closures_total` exportadas por Prometheus.

1. **Gráfico de tasas de reconexión**
   - Consulta: `rate(ws_reconnections_total[5m])`
   - Umbral sugerido de alerta: `> 0.1`.
2. **Gráfico de cierres inesperados**
   - Consulta: `rate(ws_unexpected_closures_total[5m])`
   - Umbral sugerido de alerta: `> 0.05`.

Configura alertas en Grafana para disparar notificaciones cuando las tasas superen los umbrales definidos.