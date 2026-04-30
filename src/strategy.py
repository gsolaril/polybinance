#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from dataclasses import dataclass
from asyncio import CancelledError
from typing import Any, Callable, ClassVar, Dict, List
from pandas import DataFrame, Timedelta, Timestamp
from src.models import Order, Tick, Candle
from src.market import Datafeed, Executor
from src.utils import Log, TimeFrame
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄
class On:

    callbacks = list[Callable]()
    _cron_freqs = dict[Callable, Timedelta]()
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
        cls._cron_freqs = getattr(obj, "cron", dict[Any, Any]())

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
        for method in cls._cron_freqs:
            task = cls.schedule(method)
            cls._cron_tasks.append(task)

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
            freq = cls._cron_freqs[method]
            next = Timestamp.utcnow().ceil(freq)
            while True:
                until_next = next - Timestamp.utcnow()
                wait = until_next.total_seconds()
                if (wait > 0):
                    await asyncio.sleep(wait)
                    continue
                freq = cls._cron_freqs[method]
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
        self.cron = dict[Callable, Timedelta]()
        self.orders = dict[str, Dict[str, Any]]()
        self.datafeed, self.executor = None, None
        self.data = None
        self.setup()
        On.bind(self)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setup(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def add_cron(self, method: Callable, freq: Timedelta):
        self.cron[method] = freq
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def link(self, datafeed: Datafeed, executor: Executor):
        self._datafeed, self._executor = datafeed, executor
        self.data = self._datafeed.history
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, order: Order):
        assert self._executor is not None, "Executor not linked"
        response: Dict[str, Any] = await self._executor.send(order)
        self.orders[order.UID] = response 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_kill(self): ...
    
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Test(Strategy):
    freq: TimeFrame = TimeFrame.H1
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setup(self):
        self.last = Timestamp.utcnow()

    #▄▄▄▄▄▄▄▄
    On.tick#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        self.last = tick.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_kill(self):
        DT_FORMAT = "%Y%m%d_%H%M%S"
        time_kill = Timestamp.utcnow()
        str_last = time_kill.strftime(DT_FORMAT)
        str_start = self._start.strftime(DT_FORMAT)
        str_timeline = f"{str_start}-{str_last}"

        candles = self.data.copy()
        ticks = candles.pop("tick")
        df: DataFrame = list[Any]()
        for queue in ticks.values():
            for tick in queue:
                df.append(tick)

        df = DataFrame(df).set_index(Tick.INDEX)
        df = df.loc[~ df["error"]].sort_index()
        df.to_csv(f"logs/{str_timeline}_ticks.csv")

        df: DataFrame = list[Any]()
        for tf_data in candles.values():
            for queue in tf_data.values():
                for candle in queue:
                    df.append(candle)

        subset = ["oa", "ob", "ca", "cb"]
        df = DataFrame(df).set_index(Candle.INDEX)
        df = df.dropna(subset = subset).sort_index()
        df.to_csv(f"logs/{str_timeline}_candles.csv")
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    strategy = Test()