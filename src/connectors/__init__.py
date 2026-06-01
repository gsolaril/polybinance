import sys, importlib
from .base import DataBus, ExecBus
from .binanceperp import BinancePerp, ExecBinancePerp, DataBinancePerp
from .polymarket import Polymarket, ExecPolymarket, DataPolymarket

exchanges = [BinancePerp, Polymarket]

ExecConnectors, DataConnectors = list(), list()
__all__ = ["DataBus", "ExecBus", "DataConnectors", "ExecConnectors"]

for exchange in exchanges:
    name = exchange.__name__
    mod_name = f"{exchange.__module__}"
    try:
        mod = sys.modules.get(mod_name)
        if mod is None: mod = importlib.import_module(mod_name)
        data_cls = getattr(mod, f"Data{exchange.__name__}", None)
        exec_cls = getattr(mod, f"Exec{exchange.__name__}", None)
        __all__.extend([name, f"Data{name}", f"Exec{name}"])
        if data_cls: DataConnectors.append(data_cls)
        if exec_cls: ExecConnectors.append(exec_cls)
    except (ImportError, AttributeError):
        continue
