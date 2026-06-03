#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from dataclasses import dataclass
from asyncio import CancelledError
from collections import OrderedDict
from typing import Any, Callable, ClassVar, Dict, List, Set
from src.connectors.base import DataBus, ExecBus
from pandas import DataFrame, Timestamp, concat
from src.models import Order, Tick, Response
from src.utils import Log, TimeFrame
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄
class On:

    callbacks = list[Callable]()
    _cron_freqs = dict[Callable, TimeFrame]()
    _cron_tasks: ClassVar[List[asyncio.Task]] = []
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def tick(cls, func: Callable):
        setattr(func, "_on_tick", True)
        return func
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄
    def bind(cls, obj: object):
        
        if getattr(obj, "_is_bound", False): return
        owner = obj if isinstance(obj, type) else type(obj)
        cron: dict = getattr(obj, "cron", dict[Any, Any]())
        
        for method, freq in cron.items():
            if isinstance(method, classmethod):
                method = method.__get__(owner, owner)
            cls._cron_freqs[method] = freq

        for name in dir(obj):
            method = getattr(obj, name, None)
            if not callable(method): continue
            function = getattr(method, "__func__", None)
            on_tick = getattr(function, "_on_tick", None)
            if on_tick: cls.callbacks.append(method)

        obj._is_bound = True

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄
    def start_cron(cls):
        cls.stop_cron()
        verbose = "Starting cron tasks:"
        for method, freq in cls._cron_freqs.items():
            cls._cron_tasks.append(cls.schedule(method))
            verbose += f"\n -> \"{method.__name__}\" ({freq.name})"
        Log.info(verbose)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄
    def stop_cron(cls):
        for task in cls._cron_tasks:
            if not task.done():
                task.cancel()
        cls._cron_tasks.clear()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def schedule(cls, method: Callable):
        
        async def loop():
            freq = cls._cron_freqs[method].value
            next = Timestamp.utcnow().ceil(freq)
            while True:
                until_next = next - Timestamp.utcnow()
                wait = until_next.total_seconds()
                if (wait > 0):
                    await asyncio.sleep(wait)
                    continue
                freq = cls._cron_freqs[method].value
                next = Timestamp.utcnow().ceil(freq)

                try: await method()
                except (CancelledError, KeyboardInterrupt): break
                except Exception as EXC: Log.exception(EXC); continue
        
        return asyncio.create_task(loop())
    
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Strategy:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):
        self._start = Timestamp.utcnow()
        self.cron = dict[Callable, TimeFrame]()
        self._orders = OrderedDict[str, dict]()
        self._data: DataBus = None
        self._exec: ExecBus = None
        self.setup()
        On.bind(self)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setup(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def add_cron(self, method: Callable, freq: TimeFrame):
        self.cron[method] = freq
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def link(self, databus: DataBus, execbus: ExecBus):
        self._data, self._exec = databus, execbus
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _link_error(self, ctype: str, venue: str = "Bus"):
        return f"\"{ctype}{venue}\" not linked"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_data(self, tf: Set = None, symbols: Dict = None,
                        until: Timestamp = None, **kwargs):
        dfs = list[DataFrame]()
        assert self._data is not None, self._link_error("Data", "Bus")
        if symbols is None: symbols = {K: set() for K in self._data._conns}
        for venue, symbol_array in symbols.items():
            assert venue in self._data._conns, self._link_error("Data", venue)
            bundle = self._data._conns[venue]._bundle
            symbol_set = {(venue, S) for S in symbol_array}
            df = bundle.get(tf, symbol_set, until, **kwargs)
            if not df.empty: dfs.append(df)
        if dfs: return concat(dfs)
        else: return DataFrame()
        
    # FIXME: REMEMBER THAT EACH POSITION WITHIN "ticks" IS A LIST, NOT A TICK OBJECT.
    # SO, YOU NEED TO FIRST ITERATE OVER THE LIST AND YIELD EACH TICK OBJECT DIRECTLY.

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def orders(self, UID: str = None):
        # TODO: Shall be updated from private WebSockets in the future
        if UID is not None: return self._orders.get(UID, None)
        return DataFrame.from_dict(self._orders, orient = "index")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _get_conn(self, venue: str):
        assert self._exec is not None, self._link_error("Exec", "Bus")
        conn = self._exec._conns.get(venue, None)
        assert conn is not None, self._link_error("Exec", venue)
        return conn

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def create_order(self, order: Order):
        try:
            conn = self._get_conn(order.venue)
            ok, response_obj = await conn.create_order(order)
            response = response_obj.__dict__
            if ok: self._orders[response["UID"]] = response
            return ok, response
        except Exception as EXC:
            Log.exception(EXC)
            return False, None

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def delete_order(self, UID: str):
        try: 
            if UID in self._orders: order: dict = self._orders[UID]
            else: Log.error(f"Order {UID} not found"); return False
            conn = self._get_conn(order["venue"])
            ok = await conn.delete_order(UID)
            if ok: self._orders.pop(UID, None)
            return ok
        except Exception as EXC:
            Log.exception(EXC)
            return False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_kill(self): ...
    
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Test(Strategy):
    freq: TimeFrame = TimeFrame.H1
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self): Strategy.__init__(self)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setup(self):
        self.last = Timestamp.utcnow()
        self.add_cron(self.test, TimeFrame.S5)

    #▄▄▄▄▄▄▄▄
    On.tick#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_tick(self, tick: Tick):
        self.last = tick.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def test(self):
        Log.debug("Testing cron...")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_kill(self):
        DT_FORMAT = "%Y%m%d_%H%M%S"
        time_kill = Timestamp.utcnow()
        str_last = time_kill.strftime(DT_FORMAT)
        str_start = self._start.strftime(DT_FORMAT)
        str_timeline = f"{str_start}-{str_last}"
        self.get_data().to_csv(f"logs/{str_timeline}_candles.csv")
        self.get_data(Tick).to_csv(f"logs/{str_timeline}_ticks.csv")
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    strategy = Test()