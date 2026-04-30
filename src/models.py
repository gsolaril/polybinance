#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy, hashlib, hmac
from pandas import Timedelta, Timestamp
from dataclasses import asdict, dataclass
from typing import Any, Dict, List
from urllib.parse import urlencode
from src.utils import Config
from src.base import Venue
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Order:
    venue: Venue; symbol: str
    size: float; price: float = None
    comment: str = None
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __dict__(self): return asdict(self)
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def signature(self, payload: Dict[str, Any]):
        query = urlencode(payload, doseq = True)
        hmac_key = Config.auth_binance.secret.encode("utf-8")
        hmac_msg = query.encode("utf-8")
        signature = hmac.new(hmac_key, hmac_msg, hashlib.sha256)
        return signature.hexdigest()

    #▄▄▄▄▄▄▄▄
    @property
    def binance(self):
        payload = {
            "timestamp": int(self.time.timestamp() * 1e3), "symbol": self.symbol,
            "side": self.side, "type": self.type, "quantity": abs(self.size),
            "recvWindow": 5000}
        if (self.price is not None):
            payload.update({"price": self.price, "timeInForce": self.mode})
        payload["signature"] = self.signature(payload)
        return payload

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):

        self.symbol = self.symbol.upper()
        self.symbol = self.symbol.replace("/", "")
        self.symbol = self.symbol.replace(":", "")
        assert (self.size != 0), "Order size cannot be 0"
        assert isinstance(self.venue, Venue), "Invalid venue"
        self.side = "BUY" if (self.size > 0) else "SELL"
        self.type = "LIMIT" if self.price else "MARKET"
        self.mode = "GTC" if self.price else "IOC"
        if not self.comment: self.comment = ""
        
        self.time = Timestamp.utcnow()
        ts = int(self.time.timestamp() * 1e6)
        self.UID = numpy.base_repr(ts, base = 36).upper()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Tick:
    venue: str; symbol: str
    pa: float; qa: float
    pb: float; qb: float
    time: Timestamp = None

    INDEX = ["venue", "symbol", "time"]
    #▄▄▄▄▄▄▄▄
    @property
    def __dict__(self):
        order = "time venue symbol pa qa pb qb pavga qavga pavgb qavgb delay error"
        return {key: self.__getattribute__(key) for key in order.split(" ")}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        self.pa, self.pb = float(self.pa), float(self.pb)
        self.qa, self.qb = float(self.qa), float(self.qb)
        assert isinstance(self.venue, Venue), "Invalid venue"
        self.error = (self.pa * self.qa == 0) | (self.pb * self.qb == 0)
        self.qavga = self.qavgb = self.pavga = self.pavgb = None
        if (self.time is None): self.time = Timestamp.utcnow()
        delay = Timestamp.utcnow() - self.time
        self.delay = int(delay.total_seconds() * 1e6)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_json(cls, data: List, venue: Venue):
        if not isinstance(data, List): data = [data]
        func = {Venue.BINANCE: cls._from_binance,
                Venue.PMARKET: cls._from_pmarket}
        return map(func[venue], data)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _from_binance(cls, data: Dict[str, Any]):
        data = data.get("data", None)
        if data is None: return None
        return cls(venue = Venue.BINANCE, symbol = data["s"][: -4],
            time = Timestamp(int(data["E"]), unit = "ms", tz = "UTC"),
            pa = data["a"], qa = data["A"], pb = data["b"], qb = data["B"])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _from_pmarket(cls, data: Dict[str, Any]):
        template = [{"price": 0.0, "size": 0.0}]
        if data is None: return None
        if (data["event_type"] != "book"): return
        A = data.get("asks", list())
        B = data.get("bids", list())
        if not A: A = template.copy()
        if not B: B = template.copy()
        A, B = A[-1], B[-1]
        tick = cls(venue = Venue.PMARKET, symbol = data.get("asset_id", None),
            time = Timestamp(int(data["timestamp"]), unit = "ms", tz = "UTC"),
            pa = A["price"], qa = A["size"], pb = B["price"], qb = B["size"])
        return tick
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
@dataclass
class Candle:
    tf: Timedelta; venue: Venue; symbol: str; volume: int = None; 
    oa: float = None; ha: float = None; la: float = None; ca: float = None
    ob: float = None; hb: float = None; lb: float = None; cb: float = None
    time: Timestamp = None

    INDEX = ["venue", "symbol", "tf", "time"]
    #▄▄▄▄▄▄▄▄
    @property
    def __dict__(self):
        order = "time tf venue symbol oa ha la ca ob hb lb cb volume"
        return {key: self.__getattribute__(key) for key in order.split(" ")}
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if not isinstance(self.tf, Timedelta): self.tf = Timedelta(self.tf)
        if self.time is None: self.time = Timestamp.utcnow().floor(self.tf)
        if (self.ha is None): self.ha = - numpy.inf
        if (self.la is None): self.la = + numpy.inf
        if (self.hb is None): self.hb = - numpy.inf
        if (self.lb is None): self.lb = + numpy.inf
        if not self.volume: self.volume = 0
        self.closed = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if (tick.time > self.time + self.tf):
            self.closed = True
        if self.closed: return
        if (tick.venue != self.venue): return
        if (tick.symbol != self.symbol): return
        if (self.oa is None): self.oa = tick.pa
        if (self.ob is None): self.ob = tick.pb
        if (tick.pa > self.ha): self.ha = tick.pa
        if (tick.pa < self.la): self.la = tick.pa
        if (tick.pb > self.hb): self.hb = tick.pb
        if (tick.pb < self.lb): self.lb = tick.pb
        self.ca, self.cb = tick.pa, tick.pb
        self.volume = self.volume + 1

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): pass