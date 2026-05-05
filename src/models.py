#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy, hashlib, hmac
from dataclasses import asdict, dataclass
from collections import defaultdict, deque
from typing import Any, Dict, List, Callable
from pandas import Series, DataFrame, Timestamp
from urllib.parse import urlencode
from utils import Config, TimeFrame
from base import Venue
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
    tf: TimeFrame; venue: Venue; symbol: str; volume: int = None; 
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
        if self.time is None:
            self.time = Timestamp.utcnow()
        self.time = self.time.floor(self.tf.value)
        if (self.ha is None): self.ha = - numpy.inf
        if (self.la is None): self.la = + numpy.inf
        if (self.hb is None): self.hb = - numpy.inf
        if (self.lb is None): self.lb = + numpy.inf
        self._time_close = self.time + self.tf.value
        if not self.volume: self.volume = 0
        self._closed = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if self.closed_at(tick.time): return
        if (tick.venue != self.venue): return
        if (tick.symbol != self.symbol): return
        if not tick.error:
            self.volume += 1
            if (self.oa is None): self.oa = tick.pa
            if (self.ob is None): self.ob = tick.pb
            if (tick.pa > self.ha): self.ha = tick.pa
            if (tick.pa < self.la): self.la = tick.pa
            if (tick.pb > self.hb): self.hb = tick.pb
            if (tick.pb < self.lb): self.lb = tick.pb
            self.ca, self.cb = tick.pa, tick.pb
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle(self, candle: "Candle"):
        if (candle.time < self.time): return
        if (candle.venue != self.venue): return
        if (candle.symbol != self.symbol): return
        if (candle._time_close > self._time_close): return
        if not candle._closed:
            self.volume += candle.volume
            if (self.oa is None): self.oa = candle.oa
            if (self.ob is None): self.ob = candle.ob
            if (candle.ha > self.ha): self.ha = candle.ha
            if (candle.la < self.la): self.la = candle.la
            if (candle.hb > self.hb): self.hb = candle.hb
            if (candle.lb < self.lb): self.lb = candle.lb
            self.ca, self.cb = candle.ca, candle.cb

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def closed_at(self, time: Timestamp = None):
        if (time is None): time = Timestamp.utcnow()
        if self._closed: return True
        if (self._time_close < time):
            self._closed = True
        return self._closed
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Bundle(defaultdict[TimeFrame, defaultdict[Any, deque]]):

    MAX_N_TICKS, MAX_N_CANDLES = 10_000_000, 500_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, fkey: Callable = None,
        n_ticks: int = None, n_candles: int = None):
        
        super().__init__(defaultdict, defaultdict)
        if n_ticks is None: n_ticks = self.MAX_N_TICKS
        if n_candles is None: n_candles = self.MAX_N_CANDLES
        self._len_ticks, self._len_candles = n_ticks, n_candles
        self._fkey, self._current = fkey, dict[Any, Candle]()
        self._ticks = self._new_queue(n_ticks)
        for tf in TimeFrame._value2member_map_:
            self[tf] = defaultdict[Any, deque](
                lambda: self._new_queue(n_candles))

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._current)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def ticks(self): return self._ticks.copy()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _new_queue(self, len: int):
        len = min(len, self.MAX_N_TICKS)
        return deque[Candle](maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        if self._fkey is not None: key = self._fkey(tick)
        else: key = (tick.venue, tick.symbol)

        if key not in self._current:
            self._current[key] = Candle(TimeFrame.S1, *key)
        
        self._current[key].on_tick(tick)
        self._ticks.append(tick.__dict__)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_freq(self):

        tf_upd, tf_opt = TimeFrame.S1, TimeFrame.D1
        for symbol, candle in self._current.items():
            if not candle.closed_at(): continue
            self._current.pop(symbol)
            self[tf_upd][symbol].append(candle)
            self._current[symbol] = Candle(tf_upd, *symbol)
        
        now = Timestamp.utcnow()
        for tf_upd, tf_opt in TimeFrame.updatable(now):
            n_candles = tf_upd.value // tf_opt.value
            time: Timestamp = now - tf_upd.value
            for key in self.symbols:
                candle_large = Candle(tf_upd, *key, time = time)
                candles_small = self[tf_opt][key][- n_candles :]
                for c in candles_small: candle_large.on_candle(c)
                self[tf_upd][key].append(candle_large)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __str__(self): raise NotImplementedError

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    bundle = Bundle(1000)
    print(bundle)

    ## TODO: Create random ticks for multiple symbols, to create a test dataset. Then:
    # - Resample the dataset into an OHLCV dataset with Pandas, using all TimeFrames...
    # - Create a "Bundle" object and iterate over the Tick objects to create the Candles.
    # - Compare both resulting arrays. They should be identical.