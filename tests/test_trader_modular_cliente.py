from dataclasses import dataclass

from core.operational_mode import OperationalMode


@dataclass
class DummyConfig:
    api_key: str | None = "demo-key"
    api_secret: str | None = "demo-secret"
    modo_real: bool = True
    binance_testnet: bool = False
    modo_operativo: OperationalMode | None = None

    def __post_init__(self) -> None:
        if self.modo_operativo is None:
            self.modo_operativo = OperationalMode.from_bool(self.modo_real)
        elif not isinstance(self.modo_operativo, OperationalMode):
            self.modo_operativo = OperationalMode.parse(
                str(self.modo_operativo),
                default=OperationalMode.from_bool(self.modo_real),
            )


def test_crear_cliente_config_activa_modo_real(monkeypatch):
    monkeypatch.setenv("BINANCE_SIMULATED", "1")
    from core import trader_modular

    cfg = DummyConfig(modo_real=True)
    client = trader_modular.crear_cliente(cfg)
    assert client.api_key == cfg.api_key
    assert client.api_secret == cfg.api_secret
    assert client.simulated is False
    assert client.testnet is False


def test_crear_cliente_config_en_simulado(monkeypatch):
    monkeypatch.setenv("BINANCE_SIMULATED", "0")
    from core import trader_modular

    cfg = DummyConfig(modo_real=False)
    client = trader_modular.crear_cliente(cfg)
    assert client.simulated is True


def test_crear_cliente_config_respecta_testnet(monkeypatch):
    monkeypatch.setenv("BINANCE_SIMULATED", "1")
    from core import trader_modular

    cfg = DummyConfig(modo_real=True, binance_testnet=True)
    client = trader_modular.crear_cliente(cfg)
    assert client.testnet is True
