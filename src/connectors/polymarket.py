#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from bidict import OrderedBidict
from dataclasses import dataclass
from collections import deque, OrderedDict
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
    ARROWS_FROM_CHAR = {"U": "↑", "D": "↓"}
    ARROWS_FROM_SIGN = {+1: "↑", -1: "↓", 0: "-"}
    OFFSET = Timedelta(0)
    STATUS = {"live": "OK"}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(cls): cls.OFFSET = Timedelta(0)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_tf(cls, symbol: str):
        tf_str = symbol.split(cls.ARROWS_FROM_CHAR["U"])[-1]
        tf_str = tf_str.split(cls.ARROWS_FROM_CHAR["D"])[-1]
        return TimeFrame[tf_str]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str):
        if not symbol[10:].isdigit():
            now = Timestamp.utcnow()
            tf = cls.symbol_to_tf(symbol)
            time = now.floor(tf.value)
            symbol = f"{symbol}{time.timestamp():.0f}"
        return cls.SYMBOLS.get(symbol, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str):
        return cls.SYMBOLS.inv.get(symbol, None)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict):
        status = response.get("status", None)
        status = cls.STATUS.get(status, None)
        if status is not None: return status
        if "canceled" in response:
            canceled = response["canceled"]
            if len(canceled): return "OK"
        if "not_canceled" in response:
            not_canceled = response["not_canceled"]
            if len(not_canceled): return "ERROR"

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

        ids = dict.fromkeys(cls.ARROWS_FROM_CHAR.values())
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[cls.ARROWS_FROM_CHAR[str(nm).strip()[0]]] = token
        if (ids[cls.ARROWS_FROM_CHAR["U"]] is None): return
        if (ids[cls.ARROWS_FROM_CHAR["D"]] is None): return
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
            for arrow in cls.ARROWS_FROM_CHAR.values():
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
        self._send_log = deque[dict](maxlen = 10000)
        self._ordermap = OrderedDict[str, str]()
        self._client, self._auth = None, auth

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        auth: Polymarket.Auth = self._auth
        Log.warning("Initializing Polymarket exec client...")
        client_secure = await AsyncSecureClient.create(private_key = auth.private_key,
          api_key = RelayerApiKey(key = auth.relayer_key, address = auth.relayer_address))
        self._client = await client_secure.setup_gasless_wallet()
        await (await self._client.setup_trading_approvals()).wait()
        await client_secure.close()
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def create_order(self, order: Order):

        now = Timestamp.utcnow()
        if (self._client is None): await self.start()
        response_obj = await self._client.place_limit_order(
            side = order.side.upper(), price = str(order.price),
            token_id = self.symbol_to_venue(order.symbol),
            size = str(int(abs(order.size) * 100)))

        response_json = response_obj.model_dump()
        self._send_log.append(response_json)
        status = self.status_to_local(response_json)
        response_json["status"] = status

        tf = self.symbol_to_tf(order.symbol)
        order.expiration = now.ceil(tf.value)
        EID = response_json.pop("order_id", None)
        response_json["EID"] = EID

        response = Response(order, **response_json)
        args = {"action": "create", "result": "OK"}

        if (EID is not None):
            self._ordermap[response.UID] = EID
        if (len(self._ordermap) > 10000):
            self._ordermap.popitem(last = False)

        ok = (status == "OK")
        if not ok: args["result"] = "error"
        log = Log.success if ok else Log.error
        verbose = self.VERBOSE.format(**args)
        log(verbose + f"\n => {response!r}")
        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def delete_order(self, UID: str):
        
        now = Timestamp.utcnow()
        if (self._client is None): await self.start()
        if (UID not in self._ordermap):
            verbose = self.VERBOSE.format(
                action = "delete", result = "error")
            Log.error(verbose + f"Invalid UID {UID}!")
            return False
        EID = self._ordermap[UID]
        if (self._client is None): await self.start(self._auth)
        response_obj = await self._client.cancel_order(order_id = EID)
        
        response_json = response_obj.model_dump()
        self._send_log.append(response_json)
        status = self.status_to_local(response_json)
        response_json["status"] = status
        
        args = {"action": "delete", "result": "OK"}

        ok = (status == "OK")
        if not ok: args["result"] = "error" 
        else: self._ordermap.pop(UID)
        log = Log.success if ok else Log.error
        verbose = self.VERBOSE.format(**args)
        log(verbose + f"\n => {UID} {EID}")
        return ok
        
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    #async def log(_, tick: Tick): return Log.debug(tick.__dict__)
    #asyncio.run(DataPolymarket(callbacks = [log]).connect())

    order = Order(price = 0.01, size = 0.01, side = "buy",
                  venue = "Polymarket",  symbol = "BTC+M5")
    asyncio.run(ExecPolymarket._send(order))