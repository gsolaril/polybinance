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
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_lower(self, candle: "Candle"):
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

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_candle_prev(cls, candle: "Candle"):
        return cls(tf = candle.tf, venue = candle.venue, symbol = candle.symbol,
                oa = candle.ca, ha = candle.ca, la = candle.ca, ca = candle.ca,
                ob = candle.cb, hb = candle.cb, lb = candle.cb, cb = candle.cb,
                volume = 0, time = candle._time_close)

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
#▄▄▄▄▄▄▄▄▄▄▄
class Bundle:

    MAX_N_TICKS, MAX_N_CANDLES = 10_000_000, 500_000
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, nt: int = None, nc: int = None, preload: dict = None):
        
        if nt is None: nt = self.MAX_N_TICKS
        if nc is None: nc = self.MAX_N_CANDLES
        if preload is None: preload = dict()

        self._len_ticks, self._len_candles = int(nt), int(nc)
        self._start, self._tick1 = Timestamp.utcnow(), None
        self._current = dict[Any, Candle]()
        self._ticks = self._queue(nt)
        self._tcount = dict()

        self._data_rec = dict()
        for tf in TimeFrame:
            self._data_rec[tf] = defaultdict[Any, deque](lambda: self._queue(24))
        self._data_all = dict()
        for tf in TimeFrame:
            self._data_all[tf] = defaultdict[Any, deque](lambda: self._queue(nc))
        for tf, symbols in preload.items():
            symbols: dict[tuple, deque] = symbols
            for key, candles in symbols.items():
                self._data_all[tf][key] = candles.copy()

    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def symbols(self): return sorted(self._current)
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def time_since_start(self): return Timestamp.utcnow() - self._start
    #▄▄▄▄▄▄▄▄▄▄
    @property#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def time_since_tick1(self): return Timestamp.utcnow() - self._tick1

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _queue(self, len: int = None):
        if not len: len = self._len_candles
        len = min(len, self.MAX_N_TICKS)
        return deque(maxlen = len)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_tick(self, tick: Tick):

        if self._tick1 is None:
            self._tick1 = tick.time

        key = (tick.venue, tick.symbol)
        if key not in self._current:
            self._tcount[key] = 0
            self._current[key] = Candle(TimeFrame.S1, *key, time = tick.time)
            for tf in TimeFrame:
                self._data_all[tf][key] = self._queue()
                self._data_rec[tf][key] = self._queue(24)
        
        self._current[key].on_tick(tick)
        self._ticks.append(tick.__dict__)
        self._tcount[key] += 1

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_freq(self, t: Timestamp = None):

        tf_upd, tf_opt = TimeFrame.S1, TimeFrame.D1
        for symbol, candle in self._current.copy().items():
            if not candle.closed_at(): continue
            self._current.pop(symbol)
            self._data_rec[tf_upd][symbol].append(candle)
            self._data_all[tf_upd][symbol].append(candle.__dict__)
            self._current[symbol] = Candle.on_candle_prev(candle)
        
        now = Timestamp.utcnow()
        if (t is None): t = now
        for tf_upd, tf_opt in TimeFrame.updatable(t):
            n_candles = tf_upd.value // tf_opt.value
            time: Timestamp = now - tf_upd.value
            for key in self.symbols:
                candle_upper = Candle(tf_upd, *key, time = time)
                for n in range(- n_candles, 0):
                    try: candle_lower = self._data_rec[tf_opt][key][n]
                    except IndexError: continue
                    candle_upper.on_candle_lower(candle_lower)
                self._data_rec[tf_upd][key].append(candle_upper)
                self._data_all[tf_upd][key].append(candle_upper.__dict__)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):

        df = dict()
        keys = dict()
        for tf, keys in self._data_all.items():
            df[tf] = dict()
            for key, candles in keys.items():
                df[tf][key] = len(candles)

        df = DataFrame.from_dict(df, orient = "index")
        df.columns = df.columns.rename(Tick.INDEX[: 2])
        df.loc["*ticks"] = Series(self._tcount)
        df["*total"] = df.sum(axis = "columns")
        df = concat((df.iloc[-1:], df.iloc[:-1]))
        report_lines = df.to_string().split("\n")
        for n_row, row in enumerate(report_lines):
            report_lines[n_row] = "    | " + row + " | "
        top, bottom = "_", "\u203E"
        report_lines.insert(0, " " * 4 + (len(row) + 4) * top)
        report_lines.append(" " * 4 + (len(row) + 4) * bottom)
        last_tick = self._ticks[-1]['time']
        report_lines.insert(0, " " * 2 + f"Time of last tick:  {last_tick:%H:%M:%S} ({last_tick - self._tick1} ago)")
        report_lines.insert(0, " " * 2 + f"Time of first tick: {self._tick1:%H:%M:%S} ({self.time_since_tick1} ago)")
        report_lines.insert(0, " " * 2 + f"Time of start:      {self._start:%H:%M:%S} ({self.time_since_start} ago)")

        return str.join("\n", report_lines)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def get(self, tf: Set[TimeFrame] = None, symbol: Set[tuple] = None, until: Timestamp = None, **kwargs):

        if not symbol: symbol = {*self._current.keys()}
        elif isinstance(symbol, tuple): symbol = {symbol}
        elif isinstance(symbol, str): symbol = {symbol}

        if until is None:
            until = Timestamp.max.tz_localize("UTC")
        n = kwargs.get("n", self._len_candles)
        since = kwargs.get("since", self._tick1)

        if tf is Tick:
            index = Tick.INDEX.copy()
            gen = self.gen_ticks(symbol, until, since, n)
        else:
            index = Candle.INDEX.copy()
            if not tf: tf = {*self._data_rec.keys()}
            elif isinstance(tf, str): tf = {TimeFrame[tf]}
            elif isinstance(tf, TimeFrame): tf = {tf}
            gen = self.gen_candles(tf, symbol, until, since, n)

        df = DataFrame(gen)
        try: return df.set_index(index).sort_index()
        except Exception as EXC:
            Log.exception(EXC); print(df); return df

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_candles(self, tfs: Set, symbols: Set, until: Timestamp, since: Timestamp, n: int):

        for tf, dtf in dict.items(self._data_all):
            if tf not in tfs: continue
            for symbol, candles in dict.items(dtf):
                if symbol not in symbols: continue
                ncmax = min(len(candles), n)
                for nc in range(- ncmax, 0):
                    candle: dict = candles[nc]
                    if (candle["time"] < since): continue
                    if (candle["time"] > until): continue
                    yield candle

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def gen_ticks(self, symbols: Set, until: Timestamp, since: Timestamp, n: int):
            ntmax = min(len(self._ticks), n)
            for nt in range(- ntmax, 0):
                tick: Dict = self._ticks[nt]
                key = (tick["venue"], tick["symbol"])
                if key not in symbols: continue
                if (tick["time"] < since): continue
                if (tick["time"] > until): continue
                yield tick

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): pass