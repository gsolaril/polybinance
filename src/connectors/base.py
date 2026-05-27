#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from aiohttp import ClientSession, WSMsgType
from pandas import Timestamp, Timedelta
from typing import Any, Dict, List, Callable
from ..utils import Log, TimeFrame
from ..models import Order, Bundle
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ConnectorMeta(type):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __new__(mcls: type, name: str, bases: tuple[type], namespace: dict[str, Any]):
        cls = super().__new__(mcls, name, bases, namespace)
        buffer = dict[str, tuple[Callable, Callable]]()
        cls.CALLBACKS = dict[Callable, Callable]()
        for key, method in namespace.items():
            if key.startswith("get_channels_"):
                key = key.replace("get_channels_", "")
                if key not in buffer: buffer[key] = [None, None]
                if callable(method): buffer[key][0] = method
            elif key.startswith("on_message_"):
                key = key.replace("on_message_", "")
                if key not in buffer: buffer[key] = [None, None]
                if callable(method): buffer[key][1] = method

        for key, (get_channels, on_message) in buffer.items():
            verbose = f"Method \"{name}.{{}}_{key}\" is not defined."
            assert get_channels is not None, verbose.format("get_channels")
            assert on_message is not None, verbose.format("on_message")
            cls.CALLBACKS[get_channels] = on_message
        return cls

#▄▄▄▄▄▄▄▄▄▄▄▄▄
class Exchange:
    VENUE = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(self): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): ...

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataConnector(metaclass = ConnectorMeta):
    
    CALLBACKS: dict[Callable, Callable]
    MAX_QUEUE_LENGTH = 2 * TimeFrame.RATIO
    IGNORE_TIMEFRAMES = ...
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):
        self._callbacks = callbacks
        self._bundle = Bundle(
            ncpf = self.MAX_QUEUE_LENGTH,
            ignore = self.IGNORE_TIMEFRAMES)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):
        try:
            tasks = [asyncio.create_task(self.on_freq())]
            for get_channels, on_message in self.CALLBACKS.items():
                task = self.connect(get_channels, on_message)
                tasks.append(asyncio.create_task(task))
            await asyncio.gather(*tasks, return_exceptions = True)
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
                        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def connect(self, on_channel: Callable, on_message: Callable):

        name = self.__class__.__name__
        URL, payload = on_channel(self)
        verbose = [f"\"{name}\" subscribing:",
          f"URL: \"{URL}\"", f"\"{payload}\""]

        async with ClientSession() as session:
            args = {"url": URL, "heartbeat": 5}
            Log.info(str.join("\n -> ", verbose))
            async with session.ws_connect(**args) as WS:
                try: await WS.send_json(payload)
                except Exception as EXC:
                    return Log.exception(f"\"{name}\" connection error", EXC)
                
                async for message in WS:
                    if (message.type == WSMsgType.TEXT):
                        try: asyncio.create_task(on_message(self, message.json()))
                        except json.JSONDecodeError:
                            Log.warning(f"\"{name}\" non-JSON: \"{message.data}\"")  
                    elif (message.type == WSMsgType.ERROR):
                        Log.exception(f"\"{name}\" WS error:", WS.exception()); break
                    else: Log.warning(f"\"{name}\" closed. Check just in case"); break

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecConnector(metaclass = ConnectorMeta):
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
            task = asyncio.create_task(connector.run())
            self._threads[connector.VENUE] = task
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