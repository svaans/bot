from .trader_modular import Trader

class TraderSimulado(Trader):
    """Alias de Trader usado en backtesting."""
    pass

__all__ = ["TraderSimulado"]