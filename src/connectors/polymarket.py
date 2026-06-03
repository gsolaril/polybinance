#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from bidict import OrderedBidict
from dataclasses import dataclass
from collections import deque
from typing import Any, List, Dict, Callable
from pandas import Timestamp, Timedelta
from eth_account import Account
from aiohttp import ClientSession, ClientWebSocketResponse
from src.connectors.base import DataConnector, ExecConnector
from src.connectors.base import Exchange, DataStream
from polymarket import AsyncSecureClient, RelayerApiKey
from src.models import Order, Tick, Response
from src.utils import CONFIG, SYMBOLS, TimeFrame, Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Polymarket(Exchange):
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
    URL_CONN = "https://clob.polymarket.com"

    TIMEFRAME_SLUGS: dict[TimeFrame, str] = {
        TimeFrame.M5: "{symbol}-updown-{tfi}-{tsu:.0f}",
        TimeFrame.M15: None, #"{symbol}-updown-{tfi}-{tsu:.0f}",
        TimeFrame.H1: None, #"{symbol}-up-to-down-{ts:%b-%d-%Y-%I%p}",
        TimeFrame.H4: None, #"{symbol}-updown-{tfi}-{tsu:.0f}",
    }
    SYMBOLS = OrderedBidict()
    MAX_SYMBOLS = 10000
    ARROWS = {"U": "↑", "D": "↓"}
    OFFSET = Timedelta(0)
    STATUS = {"live": "OK"}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(cls): cls.OFFSET = Timedelta(0)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str):
        if not symbol[10:].isdigit():
            now = Timestamp.utcnow()
            tf_str = symbol.split(cls.ARROWS["U"])[-1]
            tf_str = tf_str.split(cls.ARROWS["D"])[-1]
            time = now.floor(TimeFrame[tf_str].value)
            symbol = f"{symbol}{time.timestamp():.0f}"
        return cls.SYMBOLS.get(symbol, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str):
        return cls.SYMBOLS.inv.get(symbol, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict):
        return cls.STATUS.get(response["status"], None) 
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _parse_ids(cls, event: Dict[str, Any]):
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

        ids = dict.fromkeys(cls.ARROWS.values())
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[cls.ARROWS[str(nm).strip()[0]]] = token
        if (ids[cls.ARROWS["U"]] is None): return
        if (ids[cls.ARROWS["D"]] is None): return
        return ids

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_event(cls, session: ClientSession,
        symbol: str, tf: TimeFrame, time: Timestamp = None):

        slug = cls.TIMEFRAME_SLUGS.get(tf, None)
        if slug is None: return
        symbol = symbol.strip().lower()
        tf_str = TimeFrame.invert_nt(tf)
        if time is None: time = Timestamp.utcnow()
        ts_unix = time.floor(tf.value).timestamp()
        slug = slug.format(symbol = symbol, ts = time,
          tfs = tf.name, tfi = tf_str, tsu = ts_unix)
        url = cls.URL_IDS + "/events"
        
        args = {"url": url, "params": {"slug": slug}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def update(cls, symbols: list[str] = None):

        tasks = dict()
        if not symbols: symbols = [*SYMBOLS]
        Log.info(f"Retrieving Polymarket IDs for:\n -> " + str.join(", ", symbols))
        async with ClientSession(cls.URL_IDS + "/events/") as session:
            for tf in sorted(cls.TIMEFRAME_SLUGS):
                next = Timestamp.utcnow().ceil(tf.value)
                last = Timestamp.utcnow().floor(tf.value)
                for symbol in symbols:
                    key = (symbol, tf, last)
                    tasks[key] = cls._find_event(session, symbol, tf, last)
                    key = (symbol, tf, next)
                    tasks[key] = cls._find_event(session, symbol, tf, next)
            results = await asyncio.gather(*tasks.values())

        verbose = list()
        tasks = zip(tasks, results)
        for (symbol, tf, ts), event in tasks:
            if event is None: continue
            ids = cls._parse_ids(event)
            if ids is None: continue
            ti = Timestamp.timestamp(ts)
            for arrow in cls.ARROWS.values():
                key = f"{symbol}{arrow}{tf!r}{ti:.0f}"
                cls.SYMBOLS[key] = ids[arrow]
                verbose.append(f" • {key} => {ids[arrow]}")
                if (len(cls.SYMBOLS) >= cls.MAX_SYMBOLS):
                    cls.SYMBOLS.popitem(last = False)
            
        Log.success("Updated Polymarket IDs...\n" + str.join("\n", verbose))
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    cron = {update: TimeFrame.M5}
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
asyncio.run(Polymarket.update())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataPolymarket(Polymarket, DataConnector):

    URL_API = "https://gamma-api.polymarket.com/events"
    URL_WS = "wss://ws-subscriptions-clob.polymarket.com"
    DEFAULT_PAYLOAD = {"type": "market", "custom_feature_enabled": True}
    IGNORE_TIMEFRAMES = None
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):
        super().__init__(callbacks = callbacks,
            streams = {"clob": DataStream(
                self.__class__.__name__ + "/clob", 
                URL = self.URL_WS + "/ws/market",
                on_channel = self.on_channel_clob,
                on_message = self.on_message_clob,
                on_ping = self.on_ping)})

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ping(self, WS: ClientWebSocketResponse, sender: bool = False):
        now = Timestamp.utcnow().second
        if sender and (now % 10 == 0):
            return await WS.send_str("PING")
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_channel_clob(self, streams: set[str], *args, **kwargs):
        next = set(Polymarket.SYMBOLS.values())
        new, old = next - streams, streams - next
        payload = dict()
        #payload = {"type": "subscribe", "channels": ["book"],
        #       "assets_ids": [*Polymarket.SYMBOLS.values()] }
        if new: payload.update({"assets_ids": sorted(new),
            "channels": ["book"], "operation": "subscribe"})
        if old: payload.update({"assets_ids": sorted(old),
            "channels": ["book"], "operation": "unsubscribe"})
        if not streams: # initial subscribe, no oper
            payload.pop("operation")
            payload.update(self.DEFAULT_PAYLOAD)
        return old, new, payload

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
            symbol = symbol[: -10]
            A = entry.get("asks", list())
            B = entry.get("bids", list())
            if not A: A = template.copy()
            if not B: B = template.copy()
            ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
            ts = Timestamp.utcnow() # = ts + self.OFFSET
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