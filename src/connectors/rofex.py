#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json
from bidict import bidict
from dataclasses import dataclass
from collections import deque, OrderedDict
from typing import Any, List, Dict, Callable, Tuple
from pandas import Timestamp, Timedelta
from aiohttp import ClientSession
from src.connectors.base import DataConnector, ExecConnector
from src.connectors.base import Exchange, DataStream
from src.models import Order, Tick, Response
from src.utils import CONFIG, TimeFrame, Log
# remarkets@primary.com.ar (ask for test credentials)
# https://apihub.primary.com.ar/assets/docs/Primary-API.pdf
# https://remarkets.primary.ventures/ # Test environment
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄

class Rofex(Exchange):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @dataclass(frozen = True)
    class Auth:
        user: str; password: str; account: str
        proprietary: str = "PBCP"; market_id: str = "ROFX"

    _ROFEX = CONFIG["ROFEX"]
    AUTH = Auth(
        user = _ROFEX.get("user", _ROFEX.get("username")),
        password = _ROFEX["password"],
        account = _ROFEX["account"],
        proprietary = _ROFEX.get("proprietary", "PBCP"),
        market_id = _ROFEX.get("market_id", "ROFX"))

    URL_API = "https://api.remarkets.primary.com.ar/"
    URL_WS = "wss://api.remarkets.primary.com.ar/"
    OFFSET = Timedelta(0)
    TOKEN: str = None
    _LOCK: asyncio.Lock = None
    TIF = {"GTC": "DAY", "IOC": "IOC", "FOK": "FOK", "GTD": "GTD"}
    STATUS = {
        "OK": "OK", "NEW": "OK", "PENDING_NEW": "OK",
        "PARTIALLY_FILLED": "OK", "FILLED": "OK",
        "CANCELLED": "OK", "REJECTED": "ERROR", }

    MD_ENTRIES = ("BI", "OF")
    SYMBOLS = bidict()
    ROFEX_SYMBOLS = frozenset()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄
    def _load_symbols(cls):
        symbols = CONFIG["ROFEX"].get("symbols", "")
        symbols = [s.strip() for s in symbols.split() if s.strip()]
        cls.ROFEX_SYMBOLS = frozenset()
        for venue in symbols:
            local = venue.replace("/", "").replace(" ", "").upper()
            cls.SYMBOLS[local] = venue
            cls.ROFEX_SYMBOLS = cls.ROFEX_SYMBOLS | {local}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄
    async def ws_headers(cls):
        await cls.ensure_token()
        return {"X-Auth-Token": Rofex.TOKEN}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def ensure_token(cls, force: bool = False):
        
        if Rofex._LOCK is None:
            Rofex._LOCK = asyncio.Lock()

        async with Rofex._LOCK:
            if Rofex.TOKEN and not force: return Rofex.TOKEN
            auth: Rofex.Auth = cls.AUTH
            headers = {"X-Username": auth.user, "X-Password": auth.password}
            url = Rofex.URL_API + "auth/getToken"
            async with ClientSession() as session:
                async with session.post(url, headers = headers) as response:
                    if (response.status != 200):
                        text = await response.text()
                        raise RuntimeError(f"Rofex auth failed ({response.status}): {text}")
                    Rofex.TOKEN = response.headers.get("X-Auth-Token")
            return Rofex.TOKEN

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def rest_get(cls, path: str, params: Dict[str, Any] = None, retry: bool = True):

        await cls.ensure_token()
        url = Rofex.URL_API + path.lstrip("/")
        headers = {"X-Auth-Token": Rofex.TOKEN}
        output = {"data": None, "status": None}
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        async with ClientSession() as session:
            async with session.get(url, headers = headers, params = params) as response:
                output["status"] = response.status
                if (response.status == 401) and retry:
                    await cls.ensure_token(force = True)
                    return await cls.rest_get(path, params, retry = False)
                try: output["data"] = await response.json()
                except json.JSONDecodeError as EXC:
                    text = await response.text()
                    output.update(error = repr(EXC), data = text)
                    Log.error(f"Rofex non-JSON: \"{text}\"")
                except Exception as EXC:
                    output.update(error = repr(EXC), data = None)
                    Log.error(f"Rofex request failed: \"{EXC!r}\"")
                return output

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_params(cls, order: Order):
        auth: Rofex.Auth = cls.AUTH
        params = {
            "marketId": auth.market_id,
            "symbol": cls.symbol_to_venue(order.symbol),
            "side": order.side,
            "orderQty": int(abs(order.size)),
            "ordType": order.type,
            "timeInForce": cls.TIF.get(order.mode, order.mode),
            "account": auth.account,
            "cancelPrevious": "False",
            "iceberg": "False",
        }
        if (order.price is not None):
            params["price"] = order.price
        return params
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @staticmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _top_of_book(entry: Any):
        if entry is None: return 0.0, 0.0
        if isinstance(entry, list):
            if not entry: return 0.0, 0.0
            entry = entry[0]
        if not isinstance(entry, dict): return 0.0, 0.0
        price = entry.get("price", 0) or 0
        size = entry.get("size", 0) or 0
        return float(price), float(size)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str):
        if symbol in cls.SYMBOLS.inv: return cls.SYMBOLS.inv[symbol]
        return symbol.replace("/", "").replace(" ", "").upper()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str):
        symbol = symbol.replace("/", "").replace(" ", "").upper()
        return cls.SYMBOLS.get(symbol, symbol)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict):
        status = response.get("status", None)
        if isinstance(status, str):
            mapped = cls.STATUS.get(status.upper(), None)
            if mapped is not None: return mapped
        order = response.get("orderReport", response.get("order", {}))
        if isinstance(order, dict):
            status = order.get("status", status)
            if isinstance(status, str):
                mapped = cls.STATUS.get(status.upper(), None)
                if mapped is not None: return mapped
        if status == "OK": return "OK"
        if status == "ERROR": return "ERROR"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(cls): cls.OFFSET = Timedelta(0)

Rofex._load_symbols()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataRofex(Rofex, DataConnector):

    IGNORE_TIMEFRAMES = None
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):
        super().__init__(callbacks = callbacks,
            streams = {"md": DataStream(
                self.__class__.__name__ + "/md",
                URL = self.URL_WS,
                on_channel = self.on_channel_md,
                on_message = self.on_message_md,
                headers = self.ws_headers)})

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        await self.ensure_token()
        return await super().start()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_channel_md(self, streams: set[str], *args, **kwargs):

        next = {self.symbol_to_venue(S) for S in self.ROFEX_SYMBOLS}
        new, old = next - streams, streams - next
        if not new and not old: return old, new, None
        products = [{"symbol": sym, "marketId": self.AUTH.market_id}
            for sym in sorted(next)]
        payload = {
            "type": "smd", "level": 1, "depth": 1,
            "entries": list(self.MD_ENTRIES),
            "products": products,
        }
        return old, new, payload
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_message_md(self, data: Dict):

        if data.get("status") == "ERROR":
            Log.error(f"Rofex MD error: {data!r}")
            return
        if (data.get("type", "").upper() != "MD"): return
        instrument = data.get("instrumentId", {})
        symbol = instrument.get("symbol", None)
        if symbol is None: return
        md = data.get("marketData", {})
        pa, qa = self._top_of_book(md.get("OF"))
        pb, qb = self._top_of_book(md.get("BI"))
        ts = Timestamp.utcnow() # + self.OFFSET
        tick = Tick(time = ts,
            venue = self.VENUE, symbol = self.symbol_to_local(symbol),
            pa = pa, qa = qa, pb = pb, qb = qb)
        self._bundle.on_tick(tick)
        for callback in self._callbacks:
            result = callback(tick)
            if asyncio.iscoroutine(result):
                await result

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecRofex(Rofex, ExecConnector):

    URL_ORDER = Rofex.URL_API + "rest/order"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, auth: Rofex.Auth = None):
        if (auth is None): auth = Rofex.AUTH
        self._send_log = deque[dict](maxlen = 10000)
        self._ordermap = OrderedDict[str, tuple[str, str]]()
        self._auth = auth

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self):
        Log.warning("Initializing Rofex exec connector...")
        await self.ensure_token()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def create_order(self, order: Order):

        output = await self.rest_get("rest/order/newSingleOrder",
            params = self.to_params(order))
        response_json = output.get("data", {}) or {}
        self._send_log.append(response_json)
        status = self.status_to_local(response_json)
        response_json["status"] = status
        order_data = response_json.get("order", {})
        EID = order_data.get("clientId", None)
        proprietary = order_data.get("proprietary", self._auth.proprietary)
        response_json["EID"] = EID
        response_json["proprietary"] = proprietary
        response_json.pop("order", None)
        response = Response(order, **response_json)
        args = {"action": "create", "result": "OK"}
        if (EID is not None):
            self._ordermap[response.UID] = (EID, proprietary)
        if (len(self._ordermap) > 10000):
            self._ordermap.popitem(last = False)
        ok = (status == "OK") and (output.get("status") == 200)
        if not ok: args["result"] = "error"
        log = Log.success if ok else Log.error
        verbose = self.VERBOSE.format(**args)
        log(verbose + f"\n => {response!r}")
        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def modify_order(self, UID: str, order: Order):
        verbose = self.VERBOSE.format(action = "modify", result = "error")
        Log.error(verbose + "Not implemented on Rofex.")
        return False, None

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def delete_order(self, UID: str):
        await self.ensure_token()
        if (UID not in self._ordermap):
            verbose = self.VERBOSE.format(action = "delete", result = "error")
            Log.error(verbose + f"Invalid UID {UID}!")
            return False
        EID, proprietary = self._ordermap[UID]
        output = await self.rest_get("rest/order/cancelById",
            params = {"clOrdId": EID, "proprietary": proprietary})
        response_json = output.get("data", {}) or {}
        self._send_log.append(response_json)
        status = self.status_to_local(response_json)
        response_json["status"] = status
        args = {"action": "delete", "result": "OK"}
        ok = (status == "OK") and (output.get("status") == 200)
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
    async def log(_, tick: Tick): return Log.debug(tick.__dict__)
    asyncio.run(DataRofex(callbacks = [log]).start())
