#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, numpy
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, List
from pandas import Series, DataFrame, Timedelta, Timestamp
from pandas import Index, MultiIndex, DatetimeIndex
from src.connectors.polymarket import Polymarket
from src.models import Order, Tick, Response
from src.strategy import On, Strategy
from src.utils import Log, TimeFrame
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
@dataclass#█▄▄▄▄▄▄▄▄
class Test(Strategy):
    freq: TimeFrame = TimeFrame.H1
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