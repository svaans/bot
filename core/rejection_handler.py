from __future__ import annotations

"""Módulo para gestionar rechazos de operaciones y su registro."""

import asyncio
import os
from datetime import datetime, timezone

UTC = timezone.utc
from typing import Dict, List, Optional

import pandas as pd

from core.registro_metrico import registro_metrico
from core.auditoria import registrar_auditoria
from core.utils.utils import configurar_logger
from core.supervisor import tick

log = configurar_logger('rechazos')


class RejectionHandler:
    """Encapsula la lógica de registro y almacenamiento de rechazos."""

    def __init__(self, log_dir: str, registro_tecnico_csv: str | None = None, batch_size: int = 10) -> None:
        self.log_dir = log_dir
        self.registro_tecnico_csv = registro_tecnico_csv
        os.makedirs(os.path.join(log_dir, 'rechazos'), exist_ok=True)
        if registro_tecnico_csv:
            os.makedirs(os.path.dirname(registro_tecnico_csv), exist_ok=True)
        self._buffer: List[dict] = []
        self._batch_size = batch_size

    def registrar(
        self,
        symbol: str,
        motivo: str,
        puntaje: Optional[float] = None,
        peso_total: Optional[float] = None,
        estrategias: Optional[List[str] | Dict[str, float]] = None,
        capital: float = 0.0,
        config: Optional[dict] = None,
    ) -> None:
        """Centraliza los mensajes de rechazo de entradas."""
        mensaje = f'🔴 RECHAZO: {symbol} | Causa: {motivo}'
        if puntaje is not None:
            mensaje += f' | Puntaje: {puntaje:.2f}'
        if peso_total is not None:
            mensaje += f' | Peso: {peso_total:.2f}'
        if estrategias:
            estr = estrategias
            if isinstance(estr, dict):
                estr = list(estr.keys())
            mensaje += f' | Estrategias: {estr}'
        log.info(mensaje)
        registro = {
            'symbol': symbol,
            'motivo': motivo,
            'puntaje': puntaje,
            'peso_total': peso_total,
            'estrategias': ','.join(
                estrategias.keys() if isinstance(estrategias, dict) else estrategias
            )
            if estrategias
            else '',
        }
        self._buffer.append(registro)
        if len(self._buffer) >= self._batch_size:
            self.flush()
        registro_metrico.registrar('rechazo', registro)
        try:
            registrar_auditoria(
                symbol=symbol,
                evento='Entrada rechazada',
                resultado='rechazo',
                estrategias_activas=estrategias,
                score=puntaje,
                razon=motivo,
                capital_actual=capital,
                config_usada=config or {},
            )
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de rechazo: {e}')

    def flush(self) -> None:
        buffer = [r for r in self._buffer if r]
        if not buffer:
            return
        fecha = datetime.now(UTC).strftime('%Y%m%d')
        archivo = os.path.join(self.log_dir, 'rechazos', f'{fecha}.csv')
        df = pd.DataFrame(buffer)
        modo = 'a' if os.path.exists(archivo) else 'w'
        df.to_csv(archivo, mode=modo, header=not os.path.exists(archivo), index=False)
        self._buffer.clear()

    async def flush_periodically(self, intervalo: int, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(intervalo)
            self.flush()
            tick('rechazos_flush')

    def registrar_tecnico(
        self,
        symbol: str,
        score: float,
        puntos: Dict[str, float],
        tendencia: str,
        precio: float,
        motivo: str,
        estrategias: Dict[str, float] | None = None,
    ) -> None:
        """Guarda detalles de rechazos técnicos en un CSV separado."""
        if not self.registro_tecnico_csv:
            return
        fila = {
            'timestamp': datetime.now(UTC).isoformat(),
            'symbol': symbol,
            'puntaje_total': score,
            'indicadores_fallidos': ','.join([k for k, v in puntos.items() if not v]),
            'estado_mercado': tendencia,
            'precio': precio,
            'motivo': motivo,
            'estrategias': ','.join(estrategias.keys()) if estrategias else '',
        }
        df = pd.DataFrame([fila])
        modo = 'a' if os.path.exists(self.registro_tecnico_csv) else 'w'
        df.to_csv(
            self.registro_tecnico_csv,
            mode=modo,
            header=not os.path.exists(self.registro_tecnico_csv),
            index=False,
        )
