import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.risk.risk_manager import RiskManager


def generar_velas(n=5):
    """Genera ``n`` velas OHLC dummy."""
    return [
        {"open": 100 + i, "high": 101 + i, "low": 99 + i, "close": 100 + i}
        for i in range(n)
    ]


def generar_senales():
    """Genera señales de compra para dos símbolos."""
    return [
        {"symbol": "BTC/USDT", "precio": 100.0},
        {"symbol": "ETH/USDT", "precio": 200.0},
    ]


@pytest.mark.asyncio
async def test_kill_switch_cierra_todas_las_posiciones():
    # Generar velas y señales iniciales
    velas = generar_velas()
    senales = generar_senales()
    assert len(velas) == 5 and len(senales) == 2

    # Configurar RiskManager y OrderManager simulado
    # Bus simulado con métodos publish y subscribe
    bus = SimpleNamespace(publish=AsyncMock(), subscribe=lambda *a, **k: None)
    rm = RiskManager(umbral=0.05, bus=bus)
    om = SimpleNamespace()
    om.ordenes = {
        s["symbol"]: SimpleNamespace(precio_entrada=s["precio"])
        for s in senales
    }

    async def cerrar(symbol: str, precio: float, motivo: str):
        # Simula respuesta del exchange y cierra posición
        rm.cerrar_posicion(symbol)
        om.ordenes.pop(symbol, None)
        return True

    om.cerrar_async = AsyncMock(side_effect=cerrar)

    # Abrir posiciones en RiskManager
    for s in senales:
        rm.abrir_posicion(s["symbol"])

    # Forzar pérdidas para activar el kill_switch
    activado = await rm.kill_switch(
        om,
        drawdown_diario=0.1,
        limite_drawdown=0.05,
        perdidas_consecutivas=2,
        max_perdidas=3,
    )

    # Verificar cierre de todas las posiciones y estado final
    assert activado is True
    assert om.cerrar_async.await_count == len(senales)
    assert not om.ordenes
    assert rm.posiciones_abiertas == set()
    bus.publish.assert_awaited_once()
    args, _ = bus.publish.call_args
    assert args[0] == "notify"
    assert "Kill switch activado" in args[1]["mensaje"]