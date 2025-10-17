import os
import threading
import time
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Mapping

UTC = timezone.utc
import pandas as pd
from core.utils.io_metrics import observe_disk_write
from core.utils.logger import configurar_logger


LOG_DIR = os.getenv('LOG_DIR', 'logs')

log = configurar_logger(__name__)

class RegistroMetrico:
    """Gestiona el almacenamiento de métricas de trading con seguridad y eficiencia."""

    _REQUIRED_FIELDS = (
        "symbol",
        "estrategia",
        "exchange",
        "order_type",
        "modo",
        "latencia_ms",
    )

    _FIELD_ALIASES = {
        "symbol": ("ticker", "instrument"),
        "estrategia": ("strategy", "estrategia_id"),
        "modo": ("mode",),
        "exchange": ("venue",),
        "order_type": ("tipo_orden", "orderType"),
        "latencia_ms": ("latency_ms", "latencia"),
    }

    def __init__(self, carpeta: str | None = None, buffer_max=100) -> None:
        if carpeta is None:
            carpeta = os.path.join(LOG_DIR, 'metricas')
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.buffer = []
        self.buffer_max = buffer_max
        self.lock = threading.Lock()
        self._default_labels = self._cargar_defaults()

    @property
    def default_labels(self) -> dict[str, Any]:
        """Valores utilizados cuando faltan etiquetas obligatorias."""
        return dict(self._default_labels)

    def _cargar_defaults(self) -> dict[str, Any]:
        exchange_default = os.getenv('METRIC_DEFAULT_EXCHANGE', 'binance')
        symbol_default = os.getenv('METRIC_DEFAULT_SYMBOL', 'UNKNOWN')
        estrategia_default = os.getenv('METRIC_DEFAULT_ESTRATEGIA', 'default')
        order_type_default = os.getenv('METRIC_DEFAULT_ORDER_TYPE', 'n/a')
        modo_env = os.getenv('METRIC_DEFAULT_MODO')
        if modo_env is None:
            modo_env = 'real' if os.getenv('MODO_REAL', 'true').lower() == 'true' else 'paper'
        latencia_default = self._coerce_latencia(os.getenv('METRIC_DEFAULT_LATENCIA_MS'), 0.0)

        return {
            'symbol': symbol_default,
            'estrategia': estrategia_default,
            'exchange': exchange_default,
            'order_type': order_type_default,
            'modo': modo_env,
            'latencia_ms': latencia_default,
        }

    def _coerce_latencia(self, value: Any, fallback: float | None = None) -> float:
        try:
            latencia = float(value)
        except (TypeError, ValueError):
            return fallback if fallback is not None else self._default_labels['latencia_ms']
        if not isfinite(latencia) or latencia < 0:
            return fallback if fallback is not None else self._default_labels['latencia_ms']
        return latencia

    def _resolver_alias(self, datos: Mapping[str, Any], campo: str) -> Any:
        valor = datos.get(campo)
        if valor is not None:
            return valor
        for alias in self._FIELD_ALIASES.get(campo, ()): 
            if alias in datos:
                return datos[alias]
        return None

    def _preparar_datos(self, datos: Mapping[str, Any] | None) -> dict[str, Any]:
        if datos is None:
            datos = {}
        if not isinstance(datos, Mapping):
            raise TypeError('Los datos de métricas deben ser un mapeo de clave/valor')

        normalizado = dict(datos)
        for campo in self._REQUIRED_FIELDS:
            valor = self._resolver_alias(normalizado, campo)
            if valor is None or (isinstance(valor, str) and not valor.strip() and campo != 'latencia_ms'):
                valor = self._default_labels[campo]
            if campo == 'latencia_ms':
                valor = self._coerce_latencia(valor)
            normalizado[campo] = valor
        return normalizado
        

    def registrar(
        self,
        tipo: str,
        datos: Mapping[str, Any] | None,
        guardar_inmediatamente=False,
    ) -> None:
        """Agrega un nuevo registro. Guarda si se supera el límite o si se indica guardar ya."""
        payload = self._preparar_datos(datos)
        registro = {'timestamp': datetime.now(UTC).isoformat(), 'tipo': tipo}
        registro.update(payload)
        with self.lock:
            self.buffer.append(registro)
            debe_exportar = guardar_inmediatamente or len(self.buffer) >= self.buffer_max

        if debe_exportar:
            self.exportar()

    def exportar(self, intentos: int = 3) ->None:
        """Guarda todos los registros actuales del buffer a disco."""
        for intento in range(1, intentos + 1):
            with self.lock:
                if not self.buffer:
                    return
                df = pd.DataFrame(self.buffer)
                fecha = datetime.now(UTC).strftime('%Y%m%d')
                archivo = os.path.join(self.carpeta, f'{fecha}.csv')
                try:
                    modo = 'a' if os.path.exists(archivo) else 'w'
                    cab = not os.path.exists(archivo)
                    observe_disk_write(
                        'registro_metrico_csv',
                        archivo,
                        lambda: df.to_csv(archivo, mode=modo, header=cab, index=False),
                    )
                    self.buffer = []
                    return
                except Exception as e:
                    if intento == intentos:
                        log.warning(f'⚠️ Error exportando métricas tras {intentos} intentos: {e}')
            time.sleep(1)


registro_metrico = RegistroMetrico()
