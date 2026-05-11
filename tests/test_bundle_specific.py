#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, numpy
from tqdm import tqdm
from pathlib import Path
from pandas import read_pickle
from pandas import DataFrame, concat
from pandas import Timestamp, Timedelta
from unittest import TestCase, TextTestResult, main
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
        n = Bundle.MAX_N_TICKS
        self.bundle = Bundle(n, n)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_basics(self):
        self.assertEqual(len(self.bundle._ticks), 0)
        self.assertEqual(len(self.bundle._tcount), 0)
        self.assertEqual(len(self.bundle._current), 0)
        self.assertEqual(len(self.bundle._data_rec), len(TimeFrame))
        self.assertEqual(len(self.bundle._data_all), len(TimeFrame))
        self.assertEqual(len(self.bundle._data_rec[TimeFrame.S1]), 0)
        self.assertEqual(len(self.bundle._data_all[TimeFrame.S1]), 0)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test_1_symbol_1_tf(self):
        tf = TimeFrame.S12
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
                time += Timedelta("1ms") * ms
                tick = Tick(**for_all, time = time, pa = price, pb = price)
                ticks.append(tick), self.bundle.on_tick(tick)
            tc1s.append(tc := today + Timedelta("1s") * (s + 1))
            c1s.append(price), h1s.append(h), l1s.append(l)
            self.bundle.on_freq(tc)

        self.assertEqual(len(self.bundle._ticks), len(ticks))
        self.assertEqual(len(self.bundle._tcount), 1)
        symbols_rec = self.bundle._data_rec[TimeFrame.S1]
        symbols_all = self.bundle._data_all[TimeFrame.S1]
        self.assertEqual(len(symbols_rec), 1)
        self.assertEqual(len(symbols_all), 1)
        candles_rec = symbols_rec[key]
        candles_all = symbols_all[key]
        tcount = self.bundle._tcount[key]
        self.assertLessEqual(len(candles_rec), Bundle.BUFFER_SIZE)
        self.assertEqual(len(candles_all), ratio)
        self.assertEqual(tcount, len(ticks))

        for n in range(1 - len(candles_rec), 0):
            with self.subTest(n = n):
                self.assertEqual(candles_rec[n].oa, o1s[n])
                self.assertEqual(candles_rec[n].ha, h1s[n])
                self.assertEqual(candles_rec[n].la, l1s[n])
                self.assertEqual(candles_rec[n].ca, c1s[n])
                self.assertEqual(candles_rec[n].time, to1s[n])
                self.assertEqual(candles_rec[n].volume, len(price_diff))
                self.assertEqual(candles_rec[n]._time_close, tc1s[n])
                self.assertEqual(candles_all[n]["oa"], o1s[n])
                self.assertEqual(candles_all[n]["ha"], h1s[n])
                self.assertEqual(candles_all[n]["la"], l1s[n])
                self.assertEqual(candles_all[n]["ca"], c1s[n])
                self.assertEqual(candles_all[n]["time"], to1s[n])
                self.assertEqual(candles_all[n]["volume"], len(price_diff))

        symbols_rec = self.bundle._data_rec[tf]
        symbols_all = self.bundle._data_all[tf]
        self.assertEqual(len(symbols_rec), 1)
        self.assertEqual(len(symbols_all), 1)
        candles_rec = symbols_rec[key]
        candles_all = symbols_all[key]
        self.assertLessEqual(len(candles_rec), Bundle.BUFFER_SIZE)
        self.assertEqual(len(candles_all), 1)
        candle_rec = candles_rec[-1]
        candle_all = candles_all[-1]

        self.assertEqual(candle_rec.oa, o1s[0])
        self.assertEqual(candle_rec.ha, max(h1s))
        self.assertEqual(candle_rec.la, min(l1s))
        self.assertEqual(candle_rec.ca, c1s[-1])
        self.assertEqual(candle_rec.time, to1s[0])
        self.assertEqual(candle_rec.volume, len(price_diff) * ratio)
        self.assertEqual(candle_rec._time_close, tc1s[-1])
        self.assertEqual(candle_all["oa"], o1s[0])
        self.assertEqual(candle_all["ha"], max(h1s))
        self.assertEqual(candle_all["la"], min(l1s))
        self.assertEqual(candle_all["ca"], c1s[-1])
        self.assertEqual(candle_all["time"], to1s[0])
        self.assertEqual(candle_all["volume"], len(price_diff) * ratio)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main(verbosity = 2)

