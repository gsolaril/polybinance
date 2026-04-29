#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from src.strategy import *
from pandas import Series, DataFrame, concat
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
FREQ = "1s"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Test(Strategy):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **args):
        super().__init__()
        self.data = DataFrame()
        self.ema = args.get("ema", 10)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def update_data(self):
        new_data = DataFrame(self.ticks)
        self.ticks = deque[Tick](maxlen = self.MAX_ENTRIES)
        index_labels = ["venue", "symbol", "time"]
        new_data = new_data.set_index(index_labels)
        new_data["time"] = new_data["time"].dt.floor(Timedelta(FREQ))
        agg = {"o": "first", "h": "max", "l": "min", "c": "last"}
        new_data_ask = new_data.groupby(index_labels)["pa"].agg(agg)
        new_data_ask.columns = "ask_" + new_data_ask.columns
        new_data_bid = new_data.groupby(index_labels)["pb"].agg(agg)
        new_data_bid.columns = "bid_" + new_data_bid.columns
        new_data_bid["v"] = new_data.groupby(index_labels)["pa"].count()
        new_data = DataFrame.merge(new_data_ask, new_data_bid,
            left_index = True, right_index = True, how = "outer")
        self.data = concat((self.data, new_data), axis = "index")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @Strategy.on_freq#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def main(self, freq = Timedelta(FREQ)):
        self.update_data()
        print(self.data)
        return True