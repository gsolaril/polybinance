#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from collections import deque
from pandas import Timestamp, Timedelta
from typing import Any, Callable, Dict, List
from aiohttp import ClientSession, WSMsgType
from py_clob_client.client import ClobClient
from py_clob_client.client import OrderArgs
from src.base import Connector, Venue
from src.models import Candle, Order, Tick, Bundle
from src.utils import Config, Log, TimeFrame
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Datafeed(Connector):

    FREQ_MIN: Timedelta = min(TimeFrame._value2member_map_)
    STREAM_KEY = {Venue.BINANCE: "usdt@bookTicker", Venue.PMARKET: "book"}
    MAX_QUEUE_LENGTH = int(TimeFrame.D1.value / TimeFrame.S1.value) * 15
    QUEUE = deque[Any](maxlen = MAX_QUEUE_LENGTH)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):

        self._callbacks = callbacks
        self.tokens = dict[str, Any]()
        self.symbols = dict[str, Any]()
        self._candle = dict[str, Candle]()
        self.data = Bundle(self.MAX_QUEUE_LENGTH)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):

        symbols = str.join(", ", Config.symbols)
        Log.info(f"Retrieving Polymarket token IDs for: [{symbols}]")
        symbols, tokens = await self._map_pmarket(Config.symbols, Config.timeframes)
        self.symbols.update(symbols), self.tokens.update(tokens)
        tokens_str = str.join("\n", [f" => {key}: {id}" for key, id in self.tokens.items()])
        Log.success("Retrieved Polymarket token IDs...\n" + tokens_str)
        try: await asyncio.gather(
            asyncio.create_task(self.connect(self._callbacks, Venue.BINANCE)),
            asyncio.create_task(self.connect(self._callbacks, Venue.PMARKET)),
            asyncio.create_task(self.on_freq()), return_exceptions = False,
        )
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_freq(self):

        next = Timestamp.utcnow().floor(self.FREQ_MIN)
        sleep = self.FREQ_MIN.total_seconds() / 10
        while True:
            await asyncio.sleep(sleep)
            now = Timestamp.utcnow()
            if (now < next): continue
            next = now.floor(self.FREQ_MIN)
            self.data.on_freq()
                        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def connect(self, tasks: List[Callable], venue: Venue):

        key = self.STREAM_KEY[venue]
        if (venue == Venue.BINANCE):
            streams = [S.lower() + key for S in Config.symbols]
            stream_json = {"method": "SUBSCRIBE", "id": 1}
            stream_json["params"] = streams

        elif (venue == Venue.PMARKET):
            stream_json = {"type": "subscribe", "channels": [key]}
            stream_json["assets_ids"] = list[str](self.tokens)
            
        async with ClientSession() as session:
            args = {"url": self.URL_WS[venue], "heartbeat": 5}
            async with session.ws_connect(**args) as WS:
                Log.info(f"{venue.value} subscribing:\n -> \"{stream_json}\"")
                try: await WS.send_json(stream_json)
                except Exception as EXC:
                    return Log.exception(f"{venue.value} connection error", EXC)
                    
                async for message in WS:
                    if (message.type == WSMsgType.TEXT):
                        try: data = json.loads(message.data)
                        except json.JSONDecodeError as EXC:
                            Log.warning(f"{venue.value} non-JSON: \"{message.data}\"")
                            continue

                        ticks = Tick.from_json(data, venue)
                        if not ticks: continue
                        for tick in ticks:
                            if tick is None: continue
                            if (venue == Venue.PMARKET):
                                tick.symbol = self.tokens[tick.symbol]
                            self.data.on_tick(tick)
                            for task in tasks:
                                result = task(tick)
                                if asyncio.iscoroutine(result):
                                    asyncio.create_task(result)

                    elif (message.type == WSMsgType.ERROR):
                        Log.exception(f"{venue.value} WS error:", WS.exception()); break
                    else: Log.warning(f"{venue.value} closed. Check just in case"); break

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Executor(Connector):
    
    URL_API = {
        Venue.BINANCE: "https://fapi.binance.com/fapi/v1",
        Venue.PMARKET: "https://clob.polymarket.com"
    }
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, datafeed: Datafeed):
        self._tokens = datafeed.symbols
        self._clob: ClobClient = None
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄
    def clob(self):
        if self._clob is None:
            self._clob = ClobClient(chain_id = 137,
                host = self.URL_API[Venue.PMARKET],
                key = Config.auth_pmarket.api_key)

        return self._clob

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, order: Order):
        if (order.venue == Venue.BINANCE):
            return await self._send_binance(order)
        elif (order.venue == Venue.PMARKET):
            return await self._send_pmarket(order)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _send_binance(self, order: Order):

        url = self.URL_API[Venue.BINANCE] + "/order"
        args = {"url": url, "json": order.binance}
        args["headers"] = {"X-MBX-APIKEY": Config.auth_binance.api_key}
        
        async with ClientSession() as session:
            async with session.post(**args) as response:
                output = {"data": None, "status": response.status}
                try: output["data"] = await response.json()
                except json.JSONDecodeError as EXC:
                    text = await response.text()
                    output.update(error = repr(EXC), data = text)
                    Log.error(f"Binance non-JSON: \"{text}\"")
                except Exception as EXC:
                    output.update(error = repr(EXC), data = None)
                    Log.error(f"Binance order failed: \"{EXC!r}\"")
                return output

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _send_pmarket(self, order: Order):

        def create_order(order: Order):
            return self.clob.post_order(
                order = self.clob.create_order(OrderArgs(
                    token_id = self._tokens[order.symbol],
                    price = order.price, side = order.side,
                    size = abs(order.size) )))

        return await asyncio.to_thread(create_order, order)
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    async def log(_, tick: Tick):
        return Log.debug(tick.__dict__)
    receiver = Datafeed(callbacks = [log])
    asyncio.run(receiver.run())