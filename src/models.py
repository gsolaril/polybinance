#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy, hashlib, hmac
from dataclasses import asdict, dataclass
from collections import defaultdict, deque
from typing import Any, Dict, List, Set
from collections import OrderedDict
from pandas import Timestamp, Timedelta
from pandas import Series, DataFrame, concat
from urllib.parse import urlencode
from src.utils import Config, TimeFrame, Log
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
            #time = Timestamp.utcfromtimestamp(int(data["E"]) / 1e3),
            time = Timestamp.utcnow(), # TODO: account for secs of discrepancy from broker
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
            #time = Timestamp.utcfromtimestamp(int(data["timestamp"]) / 1e3),
            time = Timestamp.utcnow(), # TODO: account for secs of discrepancy from broker
            pa = A["price"], qa = A["size"], pb = B["price"], qb = B["size"])
        return tick
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        time = f"{self.time:%Y/%m/%d %H:%M:%S.%f}"
        return f"Tick(@ {time}) | {self.venue}.{self.symbol} | " \
                f"A:{self.pa}/{self.qa}, B:{self.pb}/{self.qb})"
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
        order = "time tf venue symbol volume oa ha la ca ob hb lb cb"
        return {key: self.__getattribute__(key) for key in order.split(" ")}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        if self.tf.name.startswith("S"): interval = f"{self.time:%Y/%m/%d %H:%M:%S}-{self._time_close:%S}"
        elif self.tf.name.startswith("M"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.name.startswith("H"): interval = f"{self.time:%Y/%m/%d %H:%M}-{self._time_close:%H:%M}"
        elif self.tf.name.startswith("D"): interval = f"{self.time:%Y/%m/%d}-{self._time_close:%Y/%m/%d}"
        else: interval = f"{self.time:%Y/%m/%d %H:%M:%S.%f}-{self._time_close:%Y/%m/%d %H:%M:%S.%f}"
        return f"Candle({self.tf.name} @ {interval} | {self.venue}.{self.symbol} | " \
            f"O:{self.oa}, H:{self.ha}, L:{self.la}, C:{self.ca} | V:{self.volume})"
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if self.time is None:
            self.time = Timestamp.utcnow()
        self._time_ltick = self.time
        self.time = self.time.floor(self.tf.value)
        self._time_close = self.time + self.tf.value
        if not self.volume: self.volume = 0

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if (tick.time < self._time_ltick): return
        if (tick.time >= self._time_close): return
        if (tick.venue != self.venue): return
        if (tick.symbol != self.symbol): return
        if tick.error: return
        self._time_ltick = tick.time
        self.volume = self.volume + 1
        if (self.oa is None): self.oa = tick.pa
        if (self.ha is None): self.ha = tick.pa
        if (self.la is None): self.la = tick.pa
        if (self.ob is None): self.ob = tick.pb
        if (self.hb is None): self.hb = tick.pb
        if (self.lb is None): self.lb = tick.pb
        if tick.pa:
            self.ha = max(self.ha, tick.pa)
            self.la = min(self.la, tick.pa)
            self.ca = tick.pa
        if tick.pb:
            self.hb = max(self.hb, tick.pb)
            self.lb = min(self.lb, tick.pb)
            self.cb = tick.pb
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_lower(self, candle: "Candle"):
        if (candle.time < self.time): return
        if (candle.venue != self.venue): return
        if (candle.symbol != self.symbol): return
        if (candle._time_ltick < self._time_ltick): return
        if (candle._time_close > self._time_close): return
        self._time_ltick = candle._time_ltick
        self.volume = self.volume + candle.volume
        if (self.oa is None): self.oa = candle.oa
        if (self.ob is None): self.ob = candle.ob
        if (self.ha is None): self.ha = candle.ha
        if (self.la is None): self.la = candle.la
        if (self.hb is None): self.hb = candle.hb
        if (self.lb is None): self.lb = candle.lb
        if candle.ha: self.ha = max(self.ha, candle.ha)
        if candle.la: self.la = min(self.la, candle.la)
        if candle.hb: self.hb = max(self.hb, candle.hb)
        if candle.lb: self.lb = min(self.lb, candle.lb)
        if candle.ca: self.ca = candle.ca
        if candle.cb: self.cb = candle.cb

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_prev(cls, candle: "Candle"):
        return cls(tf = candle.tf, venue = candle.venue, symbol = candle.symbol,
          ca = candle.ca, cb = candle.cb, volume = 0, time = candle._time_close,
          oa = None, ob = None)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄
class Bundle:

    MIN_N_TICKS, MAX_N_TICKS = 100_000, 10_000_000
    MIN_N_CANDLES, MAX_N_CANDLES = 10_000, 100_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, nt: int = None, nc: int = None, preload: dict = None):

        self._start = Timestamp.utcnow()
        self._tick_first = self._tick_last = None
        
        if nt is None: nt = self.MAX_N_TICKS
        if nc is None: nc = self.MAX_N_CANDLES
        self._NT, self._NC = int(nt), int(nc)

        self._count = dict()
        self._ticks = dict()
        self._candles = dict()
        for tf in TimeFrame:
            self._candles[tf] = dict()
        if preload is None: preload = dict()

        symbols: dict[tuple, deque] = None
        for tf, symbols in preload.items():
            for key, candles in symbols.items():
                if key not in self._candles[tf]:
                    self._candles[tf][key] = self._queue(self._NC)
                self._candles[tf][key] = candles.copy()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._count)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_start(self): return Timestamp.utcnow() - self._start
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_1(self): return Timestamp.utcnow() - self._tick_first.time
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_n(self): return Timestamp.utcnow() - self._tick_last.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue(self, len: int = None):
        if not len: len = self._NC
        len = min(len, self.MAX_N_TICKS)
        return deque(maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        self._tick_last = tick
        if self._tick_first is None:
            self._tick_first = tick

        nc = self._NC
        nt = self._NT // nc
        key = (tick.venue, tick.symbol)

        if key not in self._ticks:
            self._ticks[key] = OrderedDict()
            self._count[key] = 0
            for tf in TimeFrame:
                self._candles[tf][key] = self._queue(nc)

        close_at = tick.time + TimeFrame.MIN.value
        close_at = close_at.floor(TimeFrame.MIN.value)

        if close_at not in self._ticks[key]:
            self._ticks[key][close_at] = self._queue(nt)

        #print(tick)
        self._ticks[key][close_at].append(tick)
        self._count[key] = self._count[key] + 1

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_freq(self, time: Timestamp = None):

        if (time is None): time = Timestamp.utcnow()
        closed_at = time.floor(TimeFrame.MIN.value)
        opened_at = closed_at - TimeFrame.MIN.value
        #print("\nTICKS BETWEEN:", opened_at, "AND", closed_at, "\n")

        candles: OrderedDict = None
        for key, candles in self._ticks.items():
            ticks: deque = candles.get(closed_at, deque())
            candle = Candle(TimeFrame.MIN, *key, time = opened_at)
            for tick in ticks: candle.on_tick(tick)
            while (self._count[key] >= self._NT):
                n_drop = len(candles.popitem(last = False)[1])
                self._count[key] = self._count[key] - n_drop
            self._candles[TimeFrame.MIN][key].append(candle)
            #print(candle), print("-" * 120)

        tf_opt: TimeFrame = None
        for tf_upd, tf_opt in TimeFrame.updatable(time):
            time_candle: Timestamp = time - tf_upd.value
            n_candles = tf_upd.value // tf_opt.value
            for key in self.symbols:
                candle = Candle(tf_upd, *key, time = time_candle)
                for n in range(- n_candles, 0):
                    try: candle_lower = self._candles[tf_opt][key][n]
                    except IndexError: continue
                    candle.on_candle_lower(candle_lower)
                    #print(candle_lower)
                self._candles[tf_upd][key].append(candle)
                #print(candle), print("-" * 120)
                
        #print("=" * 120)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df = dict()
        df_lines = list[str]()
        keys = dict()
        for tf, keys in self._candles.items():
            df[tf] = dict()
            for key, candles in keys.items():
                df[tf][key] = len(candles)

        df = DataFrame.from_dict(df, orient = "index")
        report_lines = [f"  Time of start:      {self._start:%H:%M:%S} ({self.since_start} ago)"]

        if df.empty:
            report_lines.append("\n    ||| No data yet ||| ")
        else:
            report_lines.append(f"  Time of last tick:  {self._tick_last.time:%H:%M:%S} ({self.since_tick_n} ago)")
            report_lines.append(f"  Time of first tick: {self._tick_first.time:%H:%M:%S} ({self.since_tick_1} ago)")
            df.columns = df.columns.rename(Tick.INDEX[: 2])
            df.loc["*ticks"] = Series(self._count)
            df["*total"] = df.sum(axis = "columns")
            df = concat((df.iloc[-1:], df.iloc[:-1]))
            df_lines = df.to_string().split("\n")
            for nr, row in enumerate(df_lines):
                df_lines[nr] = f"    | {row} | "
            top, bottom = "_", "\u203E"
            df_lines.insert(0, " " * 4 + (len(row) + 4) * top)
            df_lines.append(" " * 4 + (len(row) + 4) * bottom)

        return str.join("\n", [*report_lines, *df_lines, ""])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get(self, tf: Set[TimeFrame] = None, symbol: Set[tuple] = None, until: Timestamp = None, **kwargs):

        if not symbol: symbol = {*self.symbols}
        elif isinstance(symbol, tuple): symbol = {symbol}
        elif isinstance(symbol, str): symbol = {symbol}

        if until is None:
            until = Timestamp.max.tz_localize("UTC")
        n = kwargs.get("n", self._NC)
        since = Timestamp.min.tz_localize("UTC")
        since = getattr(self._tick_first, "time", since)
        since: Timestamp = kwargs.get("since", since)

        if tf is Tick:
            index = Tick.INDEX.copy()
            gen = self.gen_ticks(symbol, until, since)
        else:
            index = Candle.INDEX.copy()
            if not tf: tf = {*self._candles.keys()}
            elif isinstance(tf, str): tf = {TimeFrame[tf]}
            elif isinstance(tf, TimeFrame): tf = {tf}
            gen = self.gen_candles(tf, symbol, until, since, n)

        df = DataFrame(gen)
        if not df.empty:
            df = df.set_index(index)
        return df.sort_index()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_candles(self, tfs: Set, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        dtf: dict = None
        for tf in tfs:
            for key in symbols:
                dtf = self._candles.get(tf, {})
                candles = dtf.get(key, [])
                ncmax = min(len(candles), n)
                for nc in range(- ncmax, 0):
                    candle: Candle = candles[nc]
                    if (candle._time_close < since): continue
                    if (candle.time > until): continue
                    yield candle.__dict__

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_ticks(self, symbols: Set, until: Timestamp, since: Timestamp):

        dtc: dict = None
        for key in symbols:
            dtc = self._ticks.get(key, {})
            for time, ticks in dtc.items():
                if (time < since): continue
                if (time > until): continue
                for nt in range(len(ticks)):
                    tick: Tick = ticks[nt]
                    if (tick.time < since): continue
                    if (tick.time > until): continue
                    yield tick.__dict__

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): pass