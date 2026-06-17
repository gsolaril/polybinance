#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if __name__ == "__main__":
    import sys, os
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    _here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, _root)
    if _here in sys.path: sys.path.remove(_here)
import asyncio, json, hmac, hashlib
from dataclasses import dataclass
from urllib.parse import urlencode
from typing import Any, List, Dict, Callable, Tuple
from collections import deque, OrderedDict
from pandas import Timestamp, Timedelta
from aiohttp import ClientSession, ClientWebSocketResponse
from src.connectors.base import DataConnector, ExecConnector
from src.connectors.base import Exchange, DataStream
from src.models import Order, Tick, Candle, Response
from src.utils import CONFIG, SYMBOLS, TimeFrame, Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Binance(Exchange):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @dataclass(frozen = True)
    class Auth:
        api_key: str; secret: str

    AUTH = Auth(**CONFIG["BINANCE"])
    URL_WS = ...
    URL_API = ...
    OFFSET = Timedelta(0)
    STATUS = {
        "NEW": "OK", "PARTIALLY_FILLED": "OK", "FILLED": "OK", "CANCELED": "OK",
    }

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def signature(cls, payload: Dict[str, Any]):
        hmac_key = cls.AUTH.secret.encode("utf-8")
        hmac_msg = urlencode(payload, doseq = True).encode("utf-8")
        signature = hmac.new(hmac_key, hmac_msg, hashlib.sha256)
        return signature.hexdigest()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def sign_payload(cls, payload: Dict[str, Any]):
        payload = payload.copy()
        payload.setdefault("timestamp", int(Timestamp.utcnow().timestamp() * 1e3))
        payload.setdefault("recvWindow", 5000)
        payload["signature"] = cls.signature(payload)
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_create_payload(cls, order: Order):
        payload = {
            "symbol": cls.symbol_to_venue(order.symbol),
            "side": order.side, "type": order.type,
            "quantity": abs(order.size),
        }
        if (order.price is not None):
            payload.update({"price": order.price, "timeInForce": order.mode})
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_modify_payload(cls, order: Order, order_id: str):
        payload = {
            "symbol": cls.symbol_to_venue(order.symbol),
            "side": order.side, "orderId": order_id,
            "quantity": abs(order.size),
        }
        if (order.price is not None):
            payload["price"] = order.price
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def to_cancel_payload(cls, symbol: str, order_id: str):
        return {"symbol": cls.symbol_to_venue(symbol), "orderId": order_id}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def status_to_local(cls, response: dict, http_status: int = None):
        if (http_status is not None) and (http_status >= 400):
            return "ERROR"
        if "code" in response: return "ERROR"
        status = response.get("status", None)
        if isinstance(status, str):
            mapped = cls.STATUS.get(status.upper(), None)
            if mapped is not None: return mapped
        if "orderId" in response: return "OK"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_offset(cls): cls.OFFSET = Timedelta(0)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinance(Binance, DataConnector):

    IGNORE_TIMEFRAMES = {TimeFrame.MIN}
    TICK_STREAM_PATH = ...
    KLINE_STREAM_PATH = ...
    TICK_CHANNEL_KEY = ...
    KLINE_CHANNEL_KEY = ...
    KLINE_EVENT = ...
    KLINE_SYMBOL_KEY = "ps"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, callbacks: List[Callable]):
        super().__init__(callbacks = callbacks,
            streams = {
                "ticks": DataStream(
                    self.__class__.__name__ + "/ticks",
                    URL = self.URL_WS + self.TICK_STREAM_PATH,
                    on_channel = self.on_channel_ticks,
                    on_message = self.on_message_ticks,
                    on_ping = self.on_ping),
                "klines": DataStream(
                    self.__class__.__name__ + "/klines",
                    URL = self.URL_WS + self.KLINE_STREAM_PATH,
                    on_channel = self.on_channel_klines,
                    on_message = self.on_message_klines,
                    on_ping = self.on_ping),
                }
            )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_ping(self, WS: ClientWebSocketResponse, sender: bool = False):
        if not sender: return await WS.send_str("pong")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_channel(self, streams: set[str], key: str):
        next = {S.lower() + key for S in SYMBOLS}
        new, old = next - streams, streams - next
        payload = dict()
        if new: payload.update({"method": "SUBSCRIBE", "id": 1, "params": sorted(new)})
        if old: payload.update({"method": "UNSUBSCRIBE", "id": 1, "params": sorted(old)})
        return old, new, payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_channel_ticks(self, streams: set[str], *args, **kwargs):
        return self.on_channel(streams, self.TICK_CHANNEL_KEY)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_channel_klines(self, streams: set[str], *args, **kwargs):
        return self.on_channel(streams, self.KLINE_CHANNEL_KEY)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_message_ticks(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        event = data.get("e", None)
        if (event is not None) and (event != "bookTicker"): return
        symbol = data.get("s", None)
        if symbol is None: return

        tse = data.get("E", None)
        ts = Timestamp.utcnow()
        if (tse is not None):
            ts = Timestamp.utcfromtimestamp(int(tse) / 1e3) + self.OFFSET
        tick = Tick(time = ts,
            venue = self.VENUE, symbol = self.symbol_to_local(symbol),
            pa = float(data["a"]), qa = float(data["A"]),
            pb = float(data["b"]), qb = float(data["B"]))

        self._bundle.on_tick(tick)
        for callback in self._callbacks:
            result = callback(tick)
            if asyncio.iscoroutine(result):
                await result

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def on_message_klines(self, data: Dict):
        if (data := data.get("data", None)) is None: return
        if (event := data.get("e", None)) is None: return
        if (event != self.KLINE_EVENT): return
        symbol = data.get(self.KLINE_SYMBOL_KEY, None)
        if symbol is None: return

        data = data.get("k", None)
        if data is None: return
        tse = data.get("t", None)
        tf_str = data.get("i", None)
        closed = data.get("x", False)
        if not tse or not tf_str or not closed: return
        ts = Timestamp.utcfromtimestamp(int(tse) / 1e3)
        tf = TimeFrame.invert_tn(tf_str)
        o, c = float(data["o"]), float(data["c"])
        h, l = float(data["h"]), float(data["l"])
        candle = Candle(tf = tf, venue = self.VENUE, symbol = self.symbol_to_local(symbol),
            oa = o, ob = o, ha = h, hb = h, la = l, lb = l, ca = c, cb = c,
            volume = int(data["n"]), time = ts)

        self._bundle.on_candle(candle)
        self._bundle.resample_candles(ts)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBinance(Binance, ExecConnector):

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):
        self._send_log = deque[dict](maxlen = 10000)
        self._ordermap = OrderedDict[str, tuple[str, str]]()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def start(self): return

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def send(self, method: str, path: str, payload: Dict[str, Any]):

        payload = self.sign_payload(payload)
        url = self.URL_API + path
        headers = {"X-MBX-APIKEY": self.AUTH.api_key}
        output = {"data": None, "status": None}

        async with ClientSession() as session:
            async with session.request(method, url,
                params = payload, headers = headers) as response:
                output["status"] = response.status
                try: output["data"] = await response.json()
                except json.JSONDecodeError as EXC:
                    text = await response.text()
                    output.update(error = repr(EXC), data = text)
                    Log.error(f"Binance non-JSON: \"{text}\"")
                except Exception as EXC:
                    output.update(error = repr(EXC), data = None)
                    Log.error(f"Binance request failed: \"{EXC!r}\"")
                return output

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _finish(self, order: Order, output: dict, action: str):

        response_json = output.get("data", {}) or {}
        if isinstance(response_json, str):
            response_json = {"msg": response_json}
        self._send_log.append(response_json)
        status = self.status_to_local(response_json, output.get("status"))
        response_json["status"] = status
        EID = response_json.get("orderId", None)
        if (EID is not None): EID = str(EID)
        response_json["EID"] = EID
        response = Response(order, **response_json)
        args = {"action": action, "result": "OK"}
        ok = (status == "OK") and (output.get("status") == 200)
        if not ok: args["result"] = "error"
        log = Log.success if ok else Log.error
        verbose = self.VERBOSE.format(**args)
        log(verbose + f"\n => {response!r}")
        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def create_order(self, order: Order):

        output = await self.send("POST", "/order",
            self.to_create_payload(order))
        ok, response = self._finish(order, output, "create")
        if ok and (response.EID is not None):
            self._ordermap[response.UID] = (response.EID, order.symbol)
        if (len(self._ordermap) > 10000):
            self._ordermap.popitem(last = False)
        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def modify_order(self, UID: str, order: Order):

        if (UID not in self._ordermap):
            verbose = self.VERBOSE.format(action = "modify", result = "error")
            Log.error(verbose + f"Invalid UID {UID}!")
            return False, None
        EID, _ = self._ordermap[UID]
        output = await self.send("PUT", "/order",
            self.to_modify_payload(order, EID))
        ok, response = self._finish(order, output, "modify")
        if ok and (response.EID is not None):
            response.UID = UID
            self._ordermap[UID] = (response.EID, order.symbol)
        return ok, response

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def delete_order(self, UID: str):

        if (UID not in self._ordermap):
            verbose = self.VERBOSE.format(action = "delete", result = "error")
            Log.error(verbose + f"Invalid UID {UID}!")
            return False
        EID, symbol = self._ordermap[UID]
        output = await self.send("DELETE", "/order",
            self.to_cancel_payload(symbol, EID))
        response_json = output.get("data", {}) or {}
        if isinstance(response_json, str):
            response_json = {"msg": response_json}
        self._send_log.append(response_json)
        status = self.status_to_local(response_json, output.get("status"))
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
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceCoin(Binance, Exchange):
    URL_WS = "wss://dstream.binance.com"
    URL_API = "https://dapi.binance.com/dapi/v1"
    SUFFIX = "USD_PERP"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol[: -len(cls.SUFFIX)]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol + cls.SUFFIX

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceCoin(DataBinance, BinanceCoin):

    TICK_STREAM_PATH = "/public/stream"
    KLINE_STREAM_PATH = "/market/stream"
    TICK_CHANNEL_KEY = "usd_perp@bookTicker"
    KLINE_CHANNEL_KEY = "usd_perp@continuousKline_1s"
    KLINE_EVENT = "continuous_kline"
    KLINE_SYMBOL_KEY = "ps"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBinanceCoin(ExecBinance, BinanceCoin): pass

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceSpot(Binance, Exchange):
    URL_WS = "wss://stream.binance.com:9443"
    URL_API = "https://api.binance.com/api/v3"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol[: -4]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol + "USDT"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceSpot(DataBinance, BinanceSpot):

    TICK_STREAM_PATH = "/stream"
    KLINE_STREAM_PATH = "/stream"
    TICK_CHANNEL_KEY = "usdt@bookTicker"
    KLINE_CHANNEL_KEY = "usdt@kline_1s"
    KLINE_EVENT = "kline"
    KLINE_SYMBOL_KEY = "s"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBinanceSpot(ExecBinance, BinanceSpot): pass


#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BinanceUsdm(Binance, Exchange):
    URL_WS = "wss://fstream.binance.com"
    URL_API = "https://fapi.binance.com/fapi/v1"
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_local(cls, symbol: str): return symbol[: -4]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbol_to_venue(cls, symbol: str): return symbol + "USDT"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DataBinanceUsdm(DataBinance, BinanceUsdm):

    TICK_STREAM_PATH = "/public/stream"
    KLINE_STREAM_PATH = "/market/stream"
    TICK_CHANNEL_KEY = "usdt@bookTicker"
    KLINE_CHANNEL_KEY = "usdt_perpetual@continuousKline_1s"
    KLINE_EVENT = "continuous_kline"
    KLINE_SYMBOL_KEY = "ps"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class ExecBinanceUsdm(ExecBinance, BinanceUsdm): pass

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def test_data():
    async def log(tick: Tick): return Log.debug(tick.__dict__)
    await DataBinanceUsdm(callbacks = [log]).start()

async def test_exec(): ...

if (__name__ == "__main__"):
    asyncio.run(test_data())