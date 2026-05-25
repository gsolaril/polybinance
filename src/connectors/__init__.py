from .base import DataBus, ExecBus
from .binanceperp import BinancePerp, ExecBinancePerp, DataBinancePerp
from .polymarket import Polymarket, ExecPolymarket, DataPolymarket

exchanges = [BinancePerp, Polymarket]

__all__ = ["DataBus", "ExecBus"]
for exchange in exchanges:
    name = exchange.__name__
    __all__.extend([name, f"Data{name}", f"Exec{name}"])