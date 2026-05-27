#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json, sys, os
from dataclasses import dataclass
from collections import deque
from bidict import bidict
from tqdm import tqdm
from typing import Any, List, Dict
from pandas import Timestamp, Timedelta
from aiohttp import ClientSession
from eth_account import Account
from polymarket import AcceptedOrder, AsyncSecureClient, RelayerApiKey
from src.connectors.base import Exchange, DataConnector, ExecConnector
from src.models import Order, Tick, Response
from src.utils import CONFIG, SYMBOLS, TimeFrame, Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Polymarket(Exchange):
    VENUE = "Polymarket"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @dataclass(frozen = True)
    class Auth:
        relayer_key: str
        private_key: str
        relayer_address: str
        wallet_address: str = None
        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        def __post_init__(self):
            account = Account.from_key(self.private_key)
            address = getattr(account, "address")
            if self.wallet_address is not None: return
            object.__setattr__(self, "wallet_address", address)

    AUTH = Auth(**CONFIG["POLYMARKET"])
    URL_IDS = "https://gamma-api.polymarket.com"
    URL_API = "https://clob.polymarket.com"

    SYMBOLS = bidict[str, str]()
    OFFSET = Timedelta(0)
    STATUS = {"live": "OK"}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(cls): cls.OFFSET = Timedelta(0)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol_local: str):
        return cls.SYMBOLS.get(symbol_local, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol_venue: str):
        return cls.SYMBOLS.inv.get(symbol_venue, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict):
        return cls.STATUS.get(response["status"], None)
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
        args = {"url": cls.URL_IDS + "/events", "params": {"slug": slug}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_events(cls, session: ClientSession, symbol: str, tf: TimeFrame,
                    lookback: int = 12, threads: int = 20, verbose: bool = False):

        symbol = symbol.strip().lower()
        semaphore = asyncio.Semaphore(threads)
        async def get_slug(suffix: int):
            slug = symbol + "-updown-" + TimeFrame.invert_nt(tf) + "-" + str(suffix)
            async with semaphore: return await cls._event_by_slug(session, slug)

        now = Timestamp.utcnow().timestamp()
        start = int(now - lookback * 3600)
        now = int(now - (now % tf.ts))

        batch_size = threads * 3
        suffixes = [*range(now, start, -tf.ts)]
        iterator = range(len(suffixes) // batch_size)
        if verbose: iterator = tqdm(iterator)

        for index in iterator:
            start = index * batch_size
            batch = suffixes[start : (start + batch_size)]
            futures = [get_slug(suffix) for suffix in batch]
            results = await asyncio.gather(*futures)
            for event in results:
                if not event: continue
                else: return event

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def st_split(cls, symbol: str):
        if "+" in symbol:
            symbol, tf_str = symbol.split("+")
            return (symbol, TimeFrame[tf_str], "+")
        if "-" in symbol:
            symbol, tf_str = symbol.split("-")
            return (symbol, TimeFrame[tf_str], "-")
        raise ValueError(f"\"{symbol}\" has no +/-")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(cls, lookback: int = 1):

        symbols = [*cls.SYMBOLS]
        if not symbols:
            for S in SYMBOLS:
                for T in TimeFrame.polyevents:
                    symbols.append(f"{S}+{T!r}")
                    symbols.append(f"{S}-{T!r}")
        
        Log.info(f"Retrieving Polymarket IDs for:\n -> " + str.join(", ", symbols))

        tasks = dict.fromkeys(cls.st_split(S)[: 2] for S in symbols)
        async with ClientSession(cls.URL_IDS + "/events/") as session:
            for S, T in tasks: tasks[(S, T)] = cls._find_events(
                session, symbol = S, tf = T, lookback = lookback)
            results = await asyncio.gather(*tasks.values())

        tasks = zip(tasks, results)
        verbose = list()

        for (symbol, tf), event in tasks:
            if event is None: continue
            ids = cls._extract_ids(event)
            if ids is None: continue
            cls.SYMBOLS[su := f"{symbol}+{tf!r}"] = ids["+"]
            cls.SYMBOLS[sd := f"{symbol}-{tf!r}"] = ids["-"]
            verbose.append(f"  • {su} => " + ids["+"])
            verbose.append(f"  • {sd} => " + ids["-"])
        Log.success("Updated Polymarket IDs...\n" + str.join("\n", verbose))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    cron = {update: TimeFrame.M5}
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
asyncio.run(Polymarket.update(lookback = 6))

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPolymarket(Polymarket, DataConnector):

    URL_API = "https://gamma-api.polymarket.com/events"
    URL_WS = "wss://ws-subscriptions-clob.polymarket.com"
    IGNORE_TIMEFRAMES = None

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get_channels_clob(self):
        payload = {"type": "subscribe", "channels": ["book"],
                "assets_ids": [*Polymarket.SYMBOLS.values()] }
        return self.URL_WS + "/ws/market", payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_message_clob(self, data: List[Dict]):

        template = [{"price": 0.0, "size": 0.0}]
        if not isinstance(data, list): data = [data]
        for entry in data:
            if entry is None: continue
            event = entry.get("event_type", None)
            if (event != "book"): continue
            aid = entry.get("asset_id", None)
            tse = entry.get("timestamp", None)
            if aid is None or tse is None: continue
            symbol = self.symbol_to_local(aid)
            if symbol is None: continue
            A = entry.get("asks", list())
            B = entry.get("bids", list())
            if not A: A = template.copy()
            if not B: B = template.copy()
            ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
            ts = Timestamp.utcnow() # ts + self.OFFSET
            tick = Tick(venue = self.VENUE, symbol = symbol,
                    pa = A[-1]["price"], qa = A[-1]["size"],
                    pb = B[-1]["price"], qb = B[-1]["size"],
                    time = ts)
                    
            self._bundle.on_tick(tick)
            for callback in self._callbacks:
                result = callback(tick)
                if asyncio.iscoroutine(result):
                    await result

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecPolymarket(Polymarket, ExecConnector):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, auth: Polymarket.Auth = None):
        if (auth is None): auth = Polymarket.AUTH
        self.responses = deque(maxlen = 10000)
        self.client, self.auth = None, auth

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def init_client(self, auth: Polymarket.Auth):
        Log.warning("Initializing Polymarket exec client...")
        client_secure = await AsyncSecureClient.create(private_key = auth.private_key,
          api_key = RelayerApiKey(key = auth.relayer_key, address = auth.relayer_address))
        self.client = await client_secure.setup_gasless_wallet()
        await (await self.client.setup_trading_approvals()).wait()
        await client_secure.close()
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, order: Order):
        if (self.client is None): await self.init_client(self.auth)
        response_json: dict = (await self.client.place_limit_order(
            side = order.side.upper(), price = str(order.price),
            token_id = self.symbol_to_venue(order.symbol),
            size = str(abs(order.size) * 100))).model_dump()

        print("RESPONSE JSON:\n -> %s\n" % response_json)
        response_json["status"] = self.status_to_local(response_json)
        response = Response(order, **response_json)
        self.responses.append(response)
        return response
        
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    #async def log(_, tick: Tick): return Log.debug(tick.__dict__)
    #asyncio.run(DataPolymarket(callbacks = [log]).connect())

    order = Order(price = 0.01, size = 0.01, side = "buy",
                  venue = "Polymarket",  symbol = "BTC+M5")
    asyncio.run(ExecPolymarket.send(order))