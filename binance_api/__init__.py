"""SDK asíncrono para consumir la API spot de Binance.

El paquete ofrece dos modos de operación intercambiables:

* **Simulado**: datos deterministas en memoria, ideal para tests y desarrollo
  offline.
* **Real**: integración completa con la API REST y WebSocket oficial de
  Binance (live y testnet) con firma HMAC y reconexión automática.

Las funciones públicas comparten la misma interfaz en ambos escenarios, por lo
que el resto del bot puede alternar entre ambientes sin modificar su lógica.
"""
from __future__ import annotations

__all__ = [
    "cliente",
    "websocket",
    "filters",
]
