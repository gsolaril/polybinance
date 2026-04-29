#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from market import *
from pandas import Timedelta
from collections import deque
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄
class Strategy:

    MAX_ENTRIES = 1000000
    BINANCE_TASKS = list()
    PMARKET_TASKS = list()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @staticmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_tick(task: Callable, venues: Set[Venue]):
        async def wrapper(tick: Tick):
            if (Venue.BINANCE in venues): Strategy.BINANCE_TASKS.append(task)
            elif (Venue.PMARKET in venues): Strategy.PMARKET_TASKS.append(task)
            return await task(receiver, tick)
        return wrapper

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @staticmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_freq(task: Callable):
        async def wrapper(*args, **kwargs):
            async def loop(*args, **kwargs):
                active, freq = True, kwargs["freq"]
                next = Timestamp.utcnow().ceil(freq)
                while active:
                    if (Timestamp.utcnow() <= next): continue
                    next = Timestamp.utcnow().ceil(freq)
                    active = await task()
            asyncio.create_task(loop())
        return wrapper

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):
        self.active = True
        self.ticks = deque[Tick](maxlen = self.MAX_ENTRIES)
        self.receiver = Receiver(self.BINANCE_TASKS, self.PMARKET_TASKS)
        self.executor = Executor(self.receiver)
        self.orders = dict[str, Order]()
        asyncio.run(self.receiver.run())

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @on_tick(venues = {Venue.BINANCE, Venue.PMARKET})
    async def append(self, tick: Tick):
        self.ticks.append(tick.__dict__)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, order: Order):
        response = await self.executor.send(order)
        self.orders[order.UID] = response
        return response