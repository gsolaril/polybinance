#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys
from loguru import logger as Log
from configparser import ConfigParser
from typing import Any, List, NamedTuple, Generator, Tuple
from pandas import Timestamp, Timedelta
from enum import Enum, EnumMeta
from sympy import divisors
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
PATH_MAIN = __file__.split(os.path.sep)[: -2]
PATH_MAIN = str.join(os.path.sep, PATH_MAIN)
CONFIG = ConfigParser()
CONFIG.read("config.ini")
Log.remove(0)
LOG_FILENAME = os.path.normpath(PATH_MAIN) + "/logs/{time:YYYYMMDD_HHmm!UTC}.log"
args = {"backtrace": False, "level": "DEBUG", "colorize": True, "serialize": False}
loc_format = "{module}.{function} @ {name} L{line}"

LOG_FORMAT = "<level>{time:HH:mm:ss.SSS!UTC} | %s</level>" % loc_format
Log.add(sys.stdout, format = f"[{LOG_FORMAT}] {{message}}", **args)

LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS!UTC} | %s | {level}" % loc_format
Log.add(LOG_FILENAME, format = f"[{LOG_FORMAT}] {{message}}", **args)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class _Meta(EnumMeta):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        tf1: TimeFrame; tf2: TimeFrame
        cls._DIVISORS = dict()
        cls.MIN, cls.MAX = None, None
        for tf1 in cls:
            cls._DIVISORS[tf1] = list()
            for tf2 in cls:
                if (tf1 <= tf2): continue
                if (tf1.value % tf2.value): continue
                list.append(cls._DIVISORS[tf1], tf2)
            if (cls.MIN is None) or (tf1 < cls.MIN): cls.MIN = tf1
            if (cls.MAX is None) or (tf1 > cls.MAX): cls.MAX = tf1

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TimeFrame(Enum, metaclass = _Meta):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    S1, S2, S3, S4, S5, S6, S10, S12, S15, S20, S30 \
          = [Timedelta(seconds = n) for n in divisors(60)[: -1]]
    M1, M2, M3, M4, M5, M6, M10, M12, M15, M20, M30 \
          = [Timedelta(minutes = n) for n in divisors(60)[: -1]]
    H1, H2, H3, H4, H6, H8, H12, D1 \
          = [Timedelta(hours = n) for n in divisors(24)]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __eq__(self, other: Enum): return (self.value == other.value)
    def __ne__(self, other: Enum): return (self.value != other.value)
    def __ge__(self, other: Enum): return (self.value >= other.value)
    def __le__(self, other: Enum): return (self.value <= other.value)
    def __gt__(self, other: Enum): return (self.value > other.value)
    def __lt__(self, other: Enum): return (self.value < other.value)
    def __hash__(self): return hash(self.value)
    def __repr__(self): return self.name
    def __str__(self): return self.name
    def __len__(self): return 30
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def invert(cls, tf: "TimeFrame"): return tf.name[1 :] + tf.name[0].lower()
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def from_array(cls, tfs: List["TimeFrame"]): return [cls[tf] for tf in tfs]
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def updatable(cls, time: Timestamp = None):
        if (time is None): time = Timestamp.utcnow()
        tf_div: List[TimeFrame] = None; tf_max: TimeFrame = None
        td = time.floor(cls.S1.value) - time.floor(cls.D1.value)
        for tf_max in reversed(cls):
            if not (td % tf_max.value): break
        for tf_div in cls._DIVISORS[tf_max]:
            if (tf_div == TimeFrame.S1): continue
            yield tf_div, cls._DIVISORS[tf_div][-1]
        if (tf_max != TimeFrame.S1):
            yield tf_max, cls._DIVISORS[tf_max][-1]

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
polyevents = CONFIG["STRATEGY"]["polyevents"].split(" ")
#▄▄▄▄▄▄▄▄▄▄▄
class Config:
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Auth(NamedTuple): api_key: str; secret: str
    """AUTH"""
    auth_binance: Auth = Auth(CONFIG["BINANCE"]["api_key"], CONFIG["BINANCE"]["secret"])
    auth_pmarket: Auth = Auth(CONFIG["POLYMARKET"]["api_key"], CONFIG["POLYMARKET"]["secret"])
    """STRATEGY"""
    symbols: List[str] = CONFIG["STRATEGY"]["symbols"].split(" ")
    timeframes: List[TimeFrame] = TimeFrame.from_array(polyevents)
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
if (__name__ == "__main__"):
    #Log.warning("This is a warning message.")
    #Log.error("This is an error message.")
    #Log.critical("This is a critical message.")
    #Log.success("This is a success message.")
    #Log.debug("This is a debug message.")
    #Log.trace("This is a trace message.")
    #Log.info(f"This is an info message. CONFIG:\n{CONFIG}")
    for tf in TimeFrame: print(">>", tf.name, ":", tf.value)
    print(">> MIN =", TimeFrame.MIN.name, ":", TimeFrame.MIN.value)
    print(">> MAX =", TimeFrame.MAX.name, ":", TimeFrame.MAX.value)