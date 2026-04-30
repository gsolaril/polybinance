#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from typing import Any


import json, tqdm
from aiohttp import ClientSession, WSMsgType
from py_clob_client.client import ClobClient
from py_clob_client.client import OrderArgs
from collections import deque
from pandas import Timestamp
from models import *
from base import *
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Datafeed(Connector):

    MAX_ENTRIES = 500000
    STREAM_KEY = {Venue.BINANCE: "usdt@bookTicker", Venue.PMARKET: "book"}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):

        self._callbacks = callbacks
        Log.info(f"Retrieving Polymarket token IDs for: [%s]" % str.join(", ", Config.symbols))
        self.symbols, self.tokens = asyncio.run(self._map_pmarket(Config.symbols, Config.polyevents))
        tokens_str = str.join("\n", [f" => {key}: {id}" for key, id in self.tokens.items()])
        Log.success("Retrieved Polymarket token IDs...\n" + tokens_str)

        self._candle = dict[str, Candle]()
        self.history = {"tick": dict[tuple, deque]()}
        for tf in ["1s", *Config.polyevents]:
            self.history[tf] = dict[tuple, deque]()
            self._candle[tf] = dict[tuple, Candle]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):
        try: await asyncio.gather(
            asyncio.create_task(self.connect(self._callbacks, Venue.BINANCE)),
            asyncio.create_task(self.connect(self._callbacks, Venue.PMARKET)),
        )
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(self, tick: Tick):
        
        empty = deque[Any](maxlen = self.MAX_ENTRIES)
        candles: Dict[tuple, Candle] = None
        key = (tick.venue, tick.symbol)
        if key not in self.history["tick"]:
            self.history["tick"][key] = empty.copy()

        for tf, candles in self._candle.items():

            if key not in candles:
                candles[key] = Candle(Timedelta(tf), *key)
        
            candles[key].on_tick(tick)
            if candles[key].closed:
                if key not in self.history[tf]:
                    self.history[tf][key] = empty.copy()
                queue = self.history[tf][key]
                queue.append(candles[key].__dict__)
                candles[key] = Candle(Timedelta(tf), *key)
                candles[key].on_tick(tick)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def connect(self, tasks: List[Callable], venue: Venue):

        if (venue == Venue.BINANCE): stream_json = {"method": "SUBSCRIBE", "id": 1,
            "params": [sym[0].lower() + self.STREAM_KEY[Venue.BINANCE] for sym in self.symbols]}

        elif (venue == Venue.PMARKET): stream_json = {"type": "subscribe",
            "channels": [self.STREAM_KEY[Venue.PMARKET]], "assets_ids": [*self.tokens.keys()]}
            
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
                            self.update(tick)
                            for task in tasks:
                                asyncio.create_task(task(self, tick))

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
    def __init__(self, receiver: Datafeed):
        self._tokens = receiver.symbols.copy()
        self.clob = ClobClient(chain_id = 137,
            key = Config.auth_pmarket.api_key,
            host = self.URL_API[Venue.PMARKET])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(cls, order: Order):
        if (order.venue == Venue.BINANCE):
            return await cls._send_binance(order)
        elif (order.venue == Venue.PMARKET):
            return await cls._send_pmarket(order)

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
                    size = abs(order.size))))

        return await asyncio.to_thread(create_order(order))
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    async def log_w(_, tick: Tick): return Log.warning(tick.__dict__)
    async def log_s(_, tick: Tick): return Log.success(tick.__dict__)
    receiver = Datafeed(binance_tasks = [log_w], pmarket_tasks = [log_s])
    asyncio.run(receiver.run())
    #order = Order(venue = Venue.PMARKET, symbol = "BTCUSDT", size = +1e-7, price = 10000)
    #print(order)
    #executor = Executor(receiver = receiver)
    #executor.send(order)