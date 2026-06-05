import sys, importlib
from .base import DataBus, ExecBus
from .binance import BinanceUsdm, ExecBinanceUsdm, DataBinanceUsdm
# from .binance import BinanceSpot, ExecBinanceSpot, DataBinanceSpot
# from .binance import BinanceCoin, ExecBinanceCoin, DataBinanceCoin
# from .bybit import BybitLinear, ExecBybitLinear, DataBybitLinear
# from .bybit import BybitSpot, ExecBybitSpot, DataBybitSpot
# from .bybit import BybitInverse, ExecBybitInverse, DataBybitInverse
from .polymarket import Polymarket, ExecPolymarket, DataPolymarket
# from .rofex import Rofex, ExecRofex, DataRofex

exchanges = [BinanceUsdm, Polymarket] #, BinanceSpot, BinanceCoin, BybitLinear, BybitSpot, BybitInverse, Rofex]

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
