#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from pandas import Timestamp, Timedelta
from aiohttp import WSMessage, WSMsgType
from aiohttp import ClientWebSocketResponse
from aiohttp import ClientSession
from typing import Any, Dict, List, Callable, Optional
from ..utils import Log, TimeFrame
from ..models import Order, Bundle
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExchangeMeta(type):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __new__(mcls: type, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        cls.VENUE = name
        return cls

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Exchange(metaclass = ExchangeMeta):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): ...

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataStream:

    BULLET = "\n\t-> "
    VERBOSE_WSER = "\"{name}\" {stream} failed (will retry after reconnect)"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, URL: str, on_channel: Callable,
        on_message: Callable, on_ping: Optional[Callable] = None):

        self.URL = URL
        self.name = name
        self.active = False
        self.on_channel = on_channel
        self.on_message = on_message
        self.on_ping: Callable = on_ping
        self._WS: ClientWebSocketResponse = None
        self._ping_task: asyncio.Task = None
        self.streams = set[str]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def verbose_subs(self, old: set, new: set):

        verbose = f"\"{self.name}\", reviewing streams..."
        if new: verbose += "\n => New (subs):"
        for stream in sorted(new):
            verbose += self.BULLET + stream
        if old: verbose += "\n => Old (unsub):"
        for stream in sorted(old):
            verbose += self.BULLET + stream
        return verbose

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_ping(self, *args, **kwargs):

        if not isinstance(self.on_ping, Callable): return
        try: await self.on_ping(self._WS, *args, **kwargs)
        except Exception as EXC:
            error = self.VERBOSE_WSER.format(
                name = self.name, stream = "ping")
            Log.exception(error, EXC)
            self.streams.clear()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send_channel(self, payload: list[dict]):

        if not isinstance(payload, list): payload = [payload]
        try:
            for item in payload:
                await self._WS.send_json(item)
            return True
        except Exception as EXC:
            error = self.VERBOSE_WSER.format(
                name = self.name, stream = "send")
            Log.exception(error, EXC)
            self.streams.clear()
            return False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_channel(self, *args, **kwargs):

        Log.warning(f"WS for \"{self.name}\" channel loop started.")
        self.active = True
        
        while self.active:

            await asyncio.sleep(1)
            if (self._WS is None) or self._WS.closed:
                await asyncio.sleep(0.5); continue
            elif self.streams: await self.send_ping()

            old, new, payload = self.on_channel(
                self.streams, *args, **kwargs)
            if (payload is None) or not payload: continue
            next_streams: set = (self.streams | new) - old
            if (self.streams == next_streams): continue

            if (self._WS is None) or self._WS.closed:
                self.streams.clear(); continue
            Log.info(self.verbose_subs(old, new))
            if await self.send_channel(payload):
                self.streams = next_streams.copy()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start_streams(self):

        self.active = True
        async with ClientSession() as session:
            while self.active:
                Log.info(f"\"{self.name}\" connecting to \"{self.URL}\"...")
                async with session.ws_connect(self.URL, heartbeat = 30) as WS:
                    try:
                        self._WS = WS
                        while not self.streams: await asyncio.sleep(0.5)
                        Log.info(f"\"{self.name}\" Ready for messages.")
                        async for message in self._WS: await self.read(message)
                    except Exception as EXC:
                        Log.exception(f"\"{self.name}\" WS error", EXC)
                        self.streams.clear(); self._WS = None
                        if self.active: await asyncio.sleep(2)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def read(self, message: WSMessage):

        #Log.debug(str(message.data)[:400] + "...")
        if (message.type == WSMsgType.TEXT):
            try: asyncio.create_task(self.on_message(message.json()))
            except json.JSONDecodeError:
                if (text := message.data) in {"PING", "PONG"}: pass
                Log.warning(f"\"{self.name}\" got non-JSON: \"{text}\"")
        elif (message.type == WSMsgType.ERROR): raise self._WS.exception()
        elif (message.type in {WSMsgType.CLOSED, WSMsgType.CLOSING}):
            Log.warning(f"\"{self.name}\" WS closed, reconnecting...")
        else:
            Log.warning(f"\"{self.name}\" weird type: \"{message.type}\"")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataConnector:
    
    MAX_QUEUE_LENGTH = 2 * TimeFrame.RATIO
    IGNORE_TIMEFRAMES = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable],
                 streams: dict[str, DataStream]):
        self._callbacks = callbacks
        self._streams = streams
        self._bundle = Bundle(
            ncpf = self.MAX_QUEUE_LENGTH,
            ignore = self.IGNORE_TIMEFRAMES)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):
        try:
            tasks = {"on_freq": asyncio.create_task(self.on_freq())}
            for name, stream in self._streams.items():
                tasks[name + "/on_channel"] = asyncio.create_task(
                    stream.start_channel(), name = name + "/on_channel")
                tasks[name + "/on_streams"] = asyncio.create_task(
                    stream.start_streams(), name = name + "/on_streams")
            await asyncio.gather(*tasks.values(), return_exceptions = True)
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_freq(self):

        next = Timestamp.utcnow()
        sleep = TimeFrame.MIN.ts / 20
        next = next.ceil(TimeFrame.MIN.value)
        while True:
            await asyncio.sleep(sleep)
            now = Timestamp.utcnow()
            if (now < next): continue
            last = now.floor(TimeFrame.MIN.value)
            next = now.ceil(TimeFrame.MIN.value)
            self._bundle.resample_ticks(last)
            self._bundle.resample_candles(last)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnector:
    URL_API = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, order: Order): ...
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄
class DataBus:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, connectors: List[DataConnector], callbacks: List[Callable]):
        self._conns = dict[str, DataConnector]()
        self._threads = dict[str, asyncio.Task]()
        for connector in connectors:
            name = getattr(connector, "VENUE", None)
            self._conns[name] = connector(callbacks)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):
        for connector in self._conns.values():
            name = getattr(connector, "VENUE", None)
            task = asyncio.create_task(connector.run())
            self._threads[name] = task
        await asyncio.gather(*self._threads.values())

#▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBus:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, connectors: List[ExecConnector]):
        self._conns = dict[str, ExecConnector]()
        self._threads = dict[str, asyncio.Task]()
        for connector in connectors:
            name = getattr(connector, "VENUE", None)
            self._conns[name] = connector

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    timeframes = TimeFrame.from_array(["S1", "M1", "M5", "M15", "M30", "H1", "H4", "D1"])
    symbols = ["BTC", "ETH", "SOL", "XRP", "DOGE", "DOT", "ADA", "LINK", "BCH", "LTC"]