#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from asyncio import CancelledError
from collections import OrderedDict
from dataclasses import dataclass, fields, MISSING
from typing import Any, Callable, ClassVar, Dict, List, Set
from src.connectors.base import ExecConnector, DataBus, ExecBus
from pandas import DataFrame, Timestamp, concat
from src.models import Order, Tick, Symbol
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
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Meta(type):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(cls, name: str, bases: tuple[type], attrs: dict[str, Any]):

        super().__init__(name, bases, attrs)
        if ("__post_init__" in attrs): return
        if (name == "Strategy"): return
        # inject __post_init__ in subclass if not defined
        def __post_init__(self): Strategy.__init__(self)
        cls.__post_init__ = __post_init__

#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Strategy(metaclass = Meta):
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
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄
    def defaults(cls):
        return {f.name: f.default for f in fields(cls) if f.default is not MISSING}

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
        except ExecConnector.Reject as EXC:
            ok, response = False, Log.error(EXC)
        except Exception as EXC:
            ok, response = False, Log.exception(EXC)

        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def modify_order(self, UID: str, order: Order):
        try:
            conn = self._get_conn(order.venue)
            ok, response_obj = await conn.modify_order(UID, order)
            if response_obj is None: return ok, None
            response = response_obj.__dict__
            if ok: self._orders[UID] = response
        except Exception as EXC:
            Log.exception(EXC)
            ok = False

        return ok, UID

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def delete_order(self, UID: str):
        try: 
            if UID in self._orders: order: dict = self._orders[UID]
            else: Log.error(f"Order {UID} not found"); return False
            conn = self._get_conn(order["venue"])
            ok = await conn.delete_order(UID)
            if ok: self._orders.pop(UID, None)
        except Exception as EXC:
            Log.exception(EXC)
            ok = False

        return ok, UID

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_kill(self): ...
    
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StateStrategy(Strategy):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setup(self): pass
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    strategy = Strategy()