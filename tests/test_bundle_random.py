#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import os, sys, numpy, time
from tqdm import tqdm
from pathlib import Path
from pandas import read_pickle
from pandas import DataFrame, concat
from pandas import Timestamp, Timedelta

CWD = str(Path(__file__).resolve().parents[0])
ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from src.models import Tick, Venue, Bundle
from src.utils import Log, TimeFrame
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
## TODO: Create random ticks for multiple symbols, to create a test dataset. Then:
# - Resample the dataset into an OHLCV dataset with Pandas, using all TimeFrames...
# - Create a "Bundle" object and iterate over the Tick objects to create the Candles.
# - Compare both resulting arrays. They should be identical.

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class BundleTest:

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, n_symbols: int = 5, n_ticks: int = 100000,
            ticks_per_second: float = 4, max_spread: float = 20):

        self.max_spread, self.TPS = max_spread, ticks_per_second
        self.n_symbols, self.n_ticks = n_symbols, n_ticks
        self.avg_tstep = TimeFrame.S1.value / self.TPS

        self.start = Timestamp.utcnow()
        self.fn = self.start.strftime("%Y%m%d%H%M%S")
        self.start = self.start.floor("1D")

        seed = numpy.random.random(size = n_symbols * 4)
        symbols = list(map(chr, 65 + (seed * 25).astype(int)))
        symbols = [symbols[n * 4 : (n + 1) * 4] for n in range(n_symbols)]
        self.symbols = [str.join("", sym) for sym in symbols]

        end = self.start + self.n_ticks * self.avg_tstep
        verbose = f"Generating...\n => Ticks: {self.n_ticks}, Symbols: {self.symbols}...\n "
        verbose += f">> Approximate timeline: {self.start:%Y-%m-%d} - {end:%Y-%m-%d %H:%M:%S}..."
        self.files = dict.fromkeys(["ticks_p", "candles_p", "ticks_b", "candles_b"])
        self.bundle = None
        Log.info(verbose)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def generate(self, min_pmag: float = 0.75, max_pmag: float = 4, digits: int = 7):

        dif_pmag = max_pmag - min_pmag
        seed = numpy.random.random(size = self.n_symbols)
        p_seed = numpy.pow(10.0, min_pmag + dif_pmag * seed)
        p_incr = 10.0 ** numpy.ceil(numpy.log10(p_seed) - digits)
        p_seed = numpy.floor(p_seed / p_incr) * p_incr

        data = dict()
        iterator = tqdm(self.symbols, ncols = 86)
        for n, symbol in enumerate(iterator):
            seed = numpy.random.random(size = (self.n_ticks, 2))
            df = DataFrame(seed, columns = ["pb", "spread"])
            seed = numpy.random.normal(0, 1, self.n_ticks)
            df["time"] = self.avg_tstep
            if True: df["time"] *= numpy.exp(seed) / 1.6
            df.index = df.pop("time").cumsum() + self.start
            df.index = df.index.floor("1ms")
            df["pb"] = numpy.sign(2 * df["pb"] - 1)
            df["pb"] = p_incr[n] * df["pb"].cumsum()
            df["pb"] = p_seed[n] + df["pb"]
            df["spread"] = df["spread"] * self.max_spread
            df["spread"] = p_incr[n] * df["spread"].round()
            df["pa"] = df["pb"] + df.pop("spread")
            data[(Venue.BINANCE, symbol)] = df
        
        df: DataFrame = concat(data)
        df.index = df.index.rename(Tick.INDEX)
        df_save = df.reset_index()
        df_save["venue"] = df_save["venue"].map(lambda v: v.name)
        df_save = df_save.set_index(Tick.INDEX).sort_index()
        self.files["ticks_p"] = f"{self.fn}_ticks_p.pickle"
        full_path = CWD + os.path.sep + self.files["ticks_p"]
        verbose = "Saving Pandas' ticks to file...\n => \"{path}\"..."
        Log.warning(verbose.format(path = full_path))
        df_save.to_pickle(full_path)
        df = df.swaplevel().sort_index()
        return df

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_pandas(self, df: DataFrame = None):

        if df is None:
            df = read_pickle(CWD + os.path.sep + self.files["ticks_p"])
        
        tf: TimeFrame = None
        res = dict.fromkeys(TimeFrame)
        for tf in tqdm(res, ncols = 86):
            df_raw = df.reset_index()
            df_raw["time"] = df_raw["time"].dt.floor(tf.value)
            df_raw = df_raw.set_index(Tick.INDEX)
            res[tf] = DataFrame.merge(
                df_raw.groupby(Tick.INDEX)["pa"].agg(volume = "count",
                    oa = "first", ha = "max", la = "min", ca = "last"),
                df_raw.groupby(Tick.INDEX)["pb"].agg(
                    ob = "first", hb = "max", lb = "min", cb = "last"),
                left_index = True, right_index = True, how = "outer")

        index = ["tf", *Tick.INDEX]
        res: DataFrame = concat(res, names = index)
        res = res.reset_index()
        res["tf"] = res["tf"].map(lambda t: t.name)
        res["venue"] = res["venue"].map(lambda v: v.name)
        res = res.set_index(index).sort_index()
        self.files["candles_p"] = f"{self.fn}_candles_p.pickle"
        full_path = CWD + os.path.sep + self.files["candles_p"]
        verbose = "Saving Pandas' candles to file...\n => \"{path}\"..."
        Log.warning(verbose.format(path = full_path))
        res.to_pickle(full_path)
        return res

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def on_bundle(self, df: DataFrame = None):

        if df is None:
            df = read_pickle(CWD + os.path.sep + self.files["ticks_p"])

        t_1s_curr = t_1s_prev = None
        iterator = tqdm(
            iterable = df.reset_index().iterrows(),
            total = df.shape[0], ncols = 86)

        self.bundle = Bundle()
        #print(), print("-" * 120)
        for _, row in iterator:
            tick = Tick(**row, qa = 1.0, qb = 1.0)
            t_1s_curr = tick.time.floor("1s")
            if (t_1s_prev is None):
                t_1s_prev = t_1s_curr
            dist = t_1s_curr - t_1s_prev
            n_upd = dist / TimeFrame.MIN.value
            for ns in range(int(n_upd)):
                t_1s_prev += TimeFrame.MIN.value
                try: self.bundle.on_freq(t_1s_prev)
                except Exception as EXC: Log.exception(EXC)
            self.bundle.on_tick(tick)

        ticks = self.bundle.get(Tick)
        index = ticks.index.names
        ticks = ticks.reset_index()
        ticks["venue"] = ticks["venue"].map(lambda v: v.name)
        ticks = ticks.set_index(index).sort_index()
        self.files["ticks_b"] = f"{self.fn}_ticks_b.pickle"
        full_path = CWD + os.path.sep + self.files["ticks_b"]
        verbose = "Saving Bundle's ticks to file...\n => \"{path}\"..."
        Log.warning(verbose.format(path = full_path))
        ticks.to_pickle(full_path)
        
        candles = self.bundle.get()
        index = candles.index.names
        candles = candles.reset_index()
        candles["tf"] = candles["tf"].map(lambda t: t.name)
        candles["venue"] = candles["venue"].map(lambda v: v.name)
        candles = candles.set_index(index).sort_index()
        self.files["candles_b"] = f"{self.fn}_candles_b.pickle"
        full_path = CWD + os.path.sep + self.files["candles_b"]
        verbose = "Saving Bundle's candles to file...\n => \"{path}\"..."
        Log.warning(verbose.format(path = full_path))
        candles.to_pickle(full_path)

        return ticks, candles

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    tests = [
        #{"n_symbols": 1, "n_ticks": 31, "ticks_per_second": 0.5},
        #{"n_symbols": 1, "n_ticks": 1200, "ticks_per_second": 1},
        #{"n_symbols": 1, "n_ticks": 1200, "ticks_per_second": 2},
        #{"n_symbols": 1, "n_ticks": 1200, "ticks_per_second": 5},
        #{"n_symbols": 2, "n_ticks": 1200, "ticks_per_second": 0.5},
        #{"n_symbols": 2, "n_ticks": 1200, "ticks_per_second": 1},
        #{"n_symbols": 2, "n_ticks": 1200, "ticks_per_second": 2},
        #{"n_symbols": 2, "n_ticks": 1200, "ticks_per_second": 5},
        #{"n_symbols": 1, "n_ticks": 120000, "ticks_per_second": 0.5},
        #{"n_symbols": 1, "n_ticks": 120000, "ticks_per_second": 1},
        #{"n_symbols": 1, "n_ticks": 120000, "ticks_per_second": 2},
        #{"n_symbols": 1, "n_ticks": 120000, "ticks_per_second": 5},
        {"n_symbols": 2, "n_ticks": 60000, "ticks_per_second": 0.1},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 0.25},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 0.5},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 1},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 2},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 5},
        #{"n_symbols": 2, "n_ticks": 120000, "ticks_per_second": 10},
    ]
    for n, test in enumerate(tests, 1):
        
        Log.debug(f"Running test #{n}/{len(tests)}:\n => {test}\n")
        test = BundleTest(**test)
        Log.info("\nPart 1: Generating dataset...")
        df = test.generate(min_pmag = 5, max_pmag = 5, digits = 5)

        Log.info("\nPart 2: Resampling dataset with Pandas...")
        df_res = test.on_pandas(df)

        Log.info(f"\nPart 3: Creating ticks and processing with Bundle... {df.shape[0]} ticks")
        ticks, candles = test.on_bundle(df)

        Log.success(f"Results...\nBundle:\n{test.bundle}")
        time.sleep(1)