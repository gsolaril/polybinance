#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, numpy
from tqdm import tqdm
from pathlib import Path
from pandas import read_pickle
from pandas import DataFrame, concat
from pandas import Timestamp, Timedelta
from unittest import TestCase, TextTestResult, main, skip
from unittest.mock import patch, Mock, AsyncMock

CWD = str(Path(__file__).resolve().parents[0])
ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from src.models import Tick, Venue, Candle, Bundle
from src.utils import Log, TimeFrame

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def _loguru_fail(self: TextTestResult, err: Exception, test: TestCase, type_: str):
    Log.opt(exception = err).error(f"{type_} in test: {test}")
    if (type_ != "FAIL"): self._original_addError(test, err)

orig_tb = TextTestResult

if not hasattr(orig_tb, "_original_addFailure"):
    orig_tb._original_addError = orig_tb.addError
    orig_tb.addError = lambda self, test, err: _loguru_fail(self, err, test, "ERROR")
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TestBundle(TestCase):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def setUp(self):
        self.bundle = Bundle()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_basics(self):
        self.assertEqual(len(self.bundle._ticks), 0)
        self.assertEqual(len(self.bundle._candles), len(TimeFrame))
        self.assertEqual(len(self.bundle._candles[TimeFrame.S1]), 0)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_1_symbol_1_tf(self):
        tf = TimeFrame.D1
        ratio = tf.value // TimeFrame.S1.value
        to1s, tc1s = list(), list()
        o1s, h1s, l1s, c1s = list(), list(), list(), list()
        for_all = {"venue": Venue.BINANCE, "symbol": "BTCUSDT", "qa": 1, "qb": 1}
        key = (for_all["venue"], for_all["symbol"])
        price_diff = [0, 1, 2, 1, 0, -1, 0, 1]
        today = Timestamp.utcnow().floor("1d")
        ticks = list()
        for s in range(ratio):
            h = l = None
            for ms, d in enumerate(price_diff):
                time = today + Timedelta("1s") * s
                price = 1000 + d + s
                if (ms == 0):
                    h = l = price
                    o1s.append(price)
                    to1s.append(time)
                h, l = max(h, price), min(l, price)
                time += Timedelta("1ms") * (1 + ms)
                tick = Tick(**for_all, time = time, pa = price, pb = price)
                ticks.append(tick), self.bundle.on_tick(tick)
            tc1s.append(tc := today + Timedelta("1s") * (s + 1))
            c1s.append(price), h1s.append(h), l1s.append(l)
            self.bundle.on_freq(tc)

        self.assertEqual(len(self.bundle._ticks), 1)
        symbols = self.bundle._candles[TimeFrame.S1]
        self.assertEqual(len(symbols), 1)
        candles = symbols[key]
        ticks = self.bundle._ticks[key]
        self.assertEqual(len(candles), ratio)
        len_ticks = min(ratio, self.bundle._NT)
        self.assertEqual(len(ticks), len_ticks)

        for n in range(1 - len(candles), 0):
            with self.subTest(n = n):
                self.assertEqual(candles[n].oa, o1s[n])
                self.assertEqual(candles[n].ha, h1s[n])
                self.assertEqual(candles[n].la, l1s[n])
                self.assertEqual(candles[n].ca, c1s[n])
                self.assertEqual(candles[n].time, to1s[n])
                self.assertEqual(candles[n].volume, len(price_diff))
                self.assertEqual(candles[n]._time_close, tc1s[n])

        symbols = self.bundle._candles[tf]
        self.assertEqual(len(symbols), 1)
        candles = symbols[key]
        self.assertEqual(len(candles), 1)
        candle = candles[-1]

        self.assertEqual(candle.oa, o1s[0])
        self.assertEqual(candle.ha, max(h1s))
        self.assertEqual(candle.la, min(l1s))
        self.assertEqual(candle.ca, c1s[-1])
        self.assertEqual(candle.time, to1s[0])
        self.assertEqual(candle.volume, len(price_diff) * ratio)
        self.assertEqual(candle._time_close, tc1s[-1])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    
    #@skip("Skipping...")
    def test_s1_counts_ticks_before_on_freq(self):
        """Ticks in the next second must be buffered before on_freq; S1 volume matches tick rows per second."""
        base = Timestamp.utcnow().floor("1s")
        fa = {"venue": Venue.BINANCE, "symbol": "X", "qa": 1.0, "qb": 1.0}
        key = (fa["venue"], fa["symbol"])
        t0 = Tick(**fa, time = base + Timedelta(milliseconds = 100), pa = 1.0, pb = 1.0)
        t1 = Tick(**fa, time = base + Timedelta(milliseconds = 500), pa = 1.5, pb = 1.5)
        t2 = Tick(**fa, time = base + Timedelta(seconds = 1, milliseconds = 50), pa = 2.0, pb = 2.0)
        self.bundle.on_tick(t0)
        self.bundle.on_tick(t1)
        self.bundle.on_tick(t2)
        self.bundle.on_freq(base + Timedelta(seconds = 1))
        s1 = self.bundle._candles[TimeFrame.S1][key]
        self.assertEqual(len(s1), 1)
        self.assertEqual(s1[-1].volume, 2)
        self.bundle.on_freq(base + Timedelta(seconds = 2))
        self.assertEqual(len(s1), 2)
        self.assertEqual(s1[-1].volume, 1)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main(verbosity = 2)

