#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import numpy, hashlib, hmac
from dataclasses import asdict, dataclass
from collections import defaultdict, deque
from typing import Any, Dict, List, Set
from pandas import Series, DataFrame, concat
from pandas import Timestamp, Timedelta
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
        return f"Candle({self.tf.name} @ {interval}) | {self.venue}.{self.symbol}" \
        f" | O:{self.oa}, H:{self.ha}, L:{self.la}, C:{self.ca} | V:{self.volume})"
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __post_init__(self):
        if self.time is None:
            self.time = Timestamp.utcnow()
        self._time_ltick = self.time
        self.time = self.time.floor(self.tf.value)
        self._time_close = self.time + self.tf.value
        if (self.ha is None): self.ha = - numpy.inf
        if (self.la is None): self.la = + numpy.inf
        if (self.hb is None): self.hb = - numpy.inf
        if (self.lb is None): self.lb = + numpy.inf
        if not self.volume: self.volume = 0

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):
        if (tick.time < self.time): return
        if (tick.time < self._time_ltick): return
        if (tick.time >= self._time_close): return
        if (tick.venue != self.venue): return
        if (tick.symbol != self.symbol): return
        if not tick.error:
            self.volume += 1
            self._time_ltick = tick.time
            if (self.oa is None): self.oa = tick.pa
            if (self.ob is None): self.ob = tick.pb
            if (tick.pa > self.ha): self.ha = tick.pa
            if (tick.pa < self.la): self.la = tick.pa
            if (tick.pb > self.hb): self.hb = tick.pb
            if (tick.pb < self.lb): self.lb = tick.pb
            self.ca, self.cb = tick.pa, tick.pb
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_lower(self, candle: "Candle"):
        if (candle.time < self.time): return
        if (candle.venue != self.venue): return
        if (candle.symbol != self.symbol): return
        if (candle._time_ltick < self._time_ltick): return
        if (candle._time_close > self._time_close): return
        self.volume += candle.volume
        self._time_ltick = candle._time_ltick
        if (self.oa is None): self.oa = candle.oa
        if (self.ob is None): self.ob = candle.ob
        if (candle.ha > self.ha): self.ha = candle.ha
        if (candle.la < self.la): self.la = candle.la
        if (candle.hb > self.hb): self.hb = candle.hb
        if (candle.lb < self.lb): self.lb = candle.lb
        self.ca, self.cb = candle.ca, candle.cb

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

    BUFFER_SIZE = 60
    MIN_N_TICKS, MAX_N_TICKS = 100_000, 10_000_000
    MIN_N_CANDLES, MAX_N_CANDLES = 10_000, 500_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, nt: int = None, nc: int = None, preload: dict = None):

        self._start = Timestamp.utcnow()
        self._tick_first = self._tick_last = None
        
        if nt is None: nt = self.MAX_N_TICKS
        if nc is None: nc = self.MAX_N_CANDLES
        self._BC, self._BT = self.BUFFER_SIZE, self.MIN_N_TICKS
        self._NT, self._NC = int(nt), int(nc)

        if preload is None: preload = dict()
        self._current = dict[Any, Candle]()

        self._cand_rec = dict()
        self._tick_rec = defaultdict[Any, deque](lambda: self._queue(self._BT))
        for tf in TimeFrame:
            self._cand_rec[tf] = defaultdict[Any, deque](lambda: self._queue(self._BC))

        self._cand_all = dict()
        self._tick_all = defaultdict[Any, deque](lambda: self._queue(self._NT))
        for tf in TimeFrame:
            self._cand_all[tf] = defaultdict[Any, deque](lambda: self._queue(self._NC))

        for tf, symbols in preload.items():
            symbols: dict[tuple, deque] = symbols
            for key, candles in symbols.items():
                self._cand_all[tf][key] = candles.copy()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._current)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_start(self): return Timestamp.utcnow() - self._start
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_1(self): return Timestamp.utcnow() - self._tick_first.time
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def since_tick_n(self): return Timestamp.utcnow() - self._tick_last.time

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue(self, len: int = None):
        if not len: len = self._NC
        len = min(len, self.MAX_N_TICKS)
        return deque(maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        if self._tick_first is None:
            self._tick_first = tick

        self._tick_last = tick
        key = (tick.venue, tick.symbol)
        if key not in self._current:
            self._current[key] = None
            self._tick_rec[key] = self._queue(self._BT)
            self._tick_all[key] = self._queue(self._NT)
            for tf in TimeFrame:
                self._cand_rec[tf][key] = self._queue(self._BC)
                self._cand_all[tf][key] = self._queue(self._NC)

        if self._current[key] is None:
            self._current[key] = Candle(TimeFrame.S1, *key, time = tick.time)

        self._current[key].on_tick(tick)
        self._tick_rec[key].append(tick)
        self._tick_all[key].append(tick)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_freq(self, time: Timestamp = None):

        tf_upd, tf_opt = TimeFrame.S1, TimeFrame.D1
        for key, candle in self._current.copy().items():
            #self._current.pop(key)
            if candle is None: continue
            self._cand_rec[tf_upd][key].append(candle)
            self._cand_all[tf_upd][key].append(candle)
            self._current[key] = None

        cpushed = dict()
        if (time is None): time = Timestamp.utcnow()
        for tf_upd, tf_opt in TimeFrame.updatable(time):
            time_candle: Timestamp = time - tf_upd.value
            n_candles = tf_upd.value // tf_opt.value
            cpushed[tf_upd] = list()
            for key in self.symbols:
                candle_upper = Candle(tf_upd, *key, time = time_candle)
                candles_lower = list()
                for n in range(- n_candles, 0):
                    try: candle_lower = self._cand_rec[tf_opt][key][n]
                    except IndexError: continue
                    candles_lower.append(candle_lower)
                    candle_upper.on_candle_lower(candle_lower)
                self._cand_rec[tf_upd][key].append(candle_upper)
                self._cand_all[tf_upd][key].append(candle_upper)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df = dict()
        df_lines = list[str]()
        keys = dict()
        for tf, keys in self._cand_all.items():
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
            tcount = {key: len(values) for key, values in self._tick_all.items()}
            df.loc["*ticks"] = Series(tcount)
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

        if not symbol: symbol = {*self._current.keys()}
        elif isinstance(symbol, tuple): symbol = {symbol}
        elif isinstance(symbol, str): symbol = {symbol}

        if until is None:
            until = Timestamp.max.tz_localize("UTC")
        n = kwargs.get("n", self._NC)
        since = kwargs.get("since", self._tick_first.time)

        if tf is Tick:
            index = Tick.INDEX.copy()
            gen = self.gen_ticks(symbol, until, since, n)
        else:
            index = Candle.INDEX.copy()
            if not tf: tf = {*self._cand_rec.keys()}
            elif isinstance(tf, str): tf = {TimeFrame[tf]}
            elif isinstance(tf, TimeFrame): tf = {tf}
            gen = self.gen_candles(tf, symbol, until, since, n)

        return DataFrame(gen).set_index(index).sort_index()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_candles(self, tfs: Set, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        for tf, dtf in dict.items(self._cand_all):
            if tf not in tfs: continue
            for key, candles in dict.items(dtf):
                if key not in symbols: continue
                ncmax = min(len(candles), n)
                for nc in range(- ncmax, 0):
                    candle: Candle = candles[nc]
                    if (candle._time_close < since): continue
                    if (candle.time > until): continue
                    yield candle.__dict__

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_ticks(self, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        for key, ticks in dict.items(self._tick_all):
            if key not in symbols: continue
            ntmax = min(len(ticks), n)
            for nt in range(- ntmax, 0):
                tick: Tick = ticks[nt]
                if (tick.time < since): continue
                if (tick.time > until): continue
                yield tick

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): pass