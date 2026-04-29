#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json, tqdm
from typing import Any
from aiohttp import ClientSession, WSMsgType
from py_clob_client.client import ClobClient
from py_clob_client.client import OrderArgs
from pandas import Timestamp
from models import *
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Connector:
    URL_WS = {
        Venue.BINANCE: "wss://fstream.binance.com/stream",
        Venue.PMARKET: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    }
    URL_API = {
        Venue.BINANCE: "https://fapi.binance.com/fapi/v1",
        Venue.PMARKET: "https://gamma-api.polymarket.com/events"
    }

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Receiver(Connector):

    STREAM_KEY = {Venue.BINANCE: "usdt@bookTicker", Venue.PMARKET: "book"}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, binance_tasks: List[Callable], pmarket_tasks: List[Callable]):

        self._binance_tasks, self._pmarket_tasks = binance_tasks, pmarket_tasks
        Log.info(f"Retrieving Polymarket token IDs for: [%s]" % str.join(", ", Config.symbols))
        self.symbols, self.tokens = asyncio.run(self._map_pmarket(Config.symbols, Config.polyevents))
        tokens_str = str.join("\n", [f" => {key}: {id}" for key, id in self.tokens.items()])
        Log.success("Retrieved Polymarket token IDs...\n" + tokens_str)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def run(self):
        try: await asyncio.gather(
            asyncio.create_task(self.connect(self._binance_tasks, Venue.BINANCE)),
            asyncio.create_task(self.connect(self._pmarket_tasks, Venue.PMARKET)),
        )
        except KeyboardInterrupt: Log.success("Exiting...")
        except Exception as EXC: Log.exception(EXC)

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
                            for task in tasks:
                                asyncio.create_task(task(tick))

                    elif (message.type == WSMsgType.ERROR):
                        Log.exception(f"{venue.value} WS error:", WS.exception()); break
                    else: Log.warning(f"{venue.value} closed. Check just in case"); break
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _tf_to_secs(cls, tf: str):
        tf = tf.strip().lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return int(tf[: -1]) * mult[tf[-1]]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _extract_ids(cls, event: Dict[str, Any]):
        markets = event.get("markets") or list()
        if not markets: return

        outcomes = markets[0].get("outcomes")
        if isinstance(outcomes, str):
            try: outcomes = json.loads(outcomes)
            except Exception: outcomes = None
        if not isinstance(outcomes, list): return

        clob_ids = markets[0].get("clobTokenIds")
        if isinstance(clob_ids, str):
            try: clob_ids = json.loads(clob_ids)
            except Exception: clob_ids = None
        if not isinstance(clob_ids, list): return

        ids = {"+": None, "-": None}
        key = {"up": "+", "down": "-"}
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[key[str(nm).strip().lower()]] = token

        if (ids["+"] is None): return
        if (ids["-"] is None): return
        return ids

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _event_by_slug(cls, session: ClientSession, slug: str):
        url = cls.URL_API[Venue.PMARKET]
        args = {"url": url, "params": {"slug": slug}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_events(cls, session: ClientSession, tf: str, symbol: str,
                lookback: int = 12, threads: int = 20, verbose: bool = False):

        tf_str = tf.strip().lower()
        symbol = symbol.strip().lower()
        semaphore = asyncio.Semaphore(threads)
        async def get_slug(suffix: int):
            slug = symbol + "-updown-" + tf_str + "-" + str(suffix)
            async with semaphore: return await cls._event_by_slug(session, slug)

        tf = cls._tf_to_secs(tf_str)
        now = Timestamp.utcnow().timestamp()
        start = int(now - lookback * 3600)
        now = int(now - (now % tf))

        batch_size = threads * 3
        suffixes = [*range(now, start, -tf)]
        iterator = range(len(suffixes) // batch_size)
        if verbose: iterator = tqdm.tqdm(iterator)

        for index in iterator:
            start = index * batch_size
            batch = suffixes[start : (start + batch_size)]
            futures = [get_slug(suffix) for suffix in batch]
            results = await asyncio.gather(*futures)
            for event in results:
                if not event: continue
                else: return event

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _map_pmarket(cls, symbols: List[str], polyevents: List[str]):
        async with ClientSession() as session:
            tasks = dict[str, Any]()
            for symbol in symbols:
                for tf in polyevents:
                    task = cls._find_events(session, tf, symbol)
                    tasks[(symbol, tf)] = task
            results = await asyncio.gather(*tasks.values())

        tasks = zip(tasks.keys(), results)
        stoe, etos = dict[str, Any](), dict[str, Any]()

        for (symbol, tf), event in tasks:
            if event is None: continue
            ids = cls._extract_ids(event)
            if ids is None: continue
            stoe[symbol + "+" + tf] = ids["+"]
            stoe[symbol + "-" + tf] = ids["-"]
            etos[ids["+"]] = symbol + "+" + tf
            etos[ids["-"]] = symbol + "-" + tf

        return stoe, etos
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
    def __init__(self, receiver: Receiver):
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
    receiver = Receiver(binance_tasks = [log_w], pmarket_tasks = [log_s])
    asyncio.run(receiver.run())
    #order = Order(venue = Venue.PMARKET, symbol = "BTCUSDT", size = +1e-7, price = 10000)
    #print(order)
    #executor = Executor(receiver = receiver)
    #executor.send(order)