#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio, json, tqdm
from typing import Any, Dict, List
from aiohttp import ClientSession
from pandas import Timestamp
from enum import StrEnum
from src.utils import Log
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Venue(StrEnum): BINANCE, PMARKET = "BINANCE", "POLYMARKET"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class Connector:
    URL_WS = {
        Venue.BINANCE: "wss://fstream.binance.com/stream",
        Venue.PMARKET: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    }
    URL_API = {
        Venue.BINANCE: "https://fapi.binance.com/fapi/v1",
        Venue.PMARKET: "https://gamma-api.polymarket.com/events"
    }
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _tf_to_secs(cls, tf: str):
        tf = tf.strip().lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return int(tf[: -1]) * mult[tf[-1]]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _extract_ids(cls, event: Dict[str, Any]):
        markets = event.get("markets") or list()
        if not markets: return

        outcomes = markets[0].get("outcomes")
        if isinstance(outcomes, str):
            try: outcomes = json.loads(outcomes)
            except Exception: outcomes = None
        if not isinstance(outcomes, list): return

        clob_ids = markets[0].get("clobTokenIds")
        if isinstance(clob_ids, str):
            try: clob_ids = json.loads(clob_ids)
            except Exception: clob_ids = None
        if not isinstance(clob_ids, list): return

        ids = {"+": None, "-": None}
        key = {"up": "+", "down": "-"}
        if len(outcomes) != len(clob_ids): return
        for nm, token in zip(outcomes, clob_ids):
            ids[key[str(nm).strip().lower()]] = token

        if (ids["+"] is None): return
        if (ids["-"] is None): return
        return ids

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _event_by_slug(cls, session: ClientSession, slug: str):
        url = cls.URL_API[Venue.PMARKET]
        args = {"url": url, "params": {"slug": slug}}
        async with session.get(**args) as resp:
            if (resp.status != 200): return
            try: data = await resp.json()
            except Exception as EXC: return Log.exception(EXC)
            if isinstance(data, list) and len(data): return data[0]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _find_events(cls, session: ClientSession, tf: str, symbol: str,
                lookback: int = 12, threads: int = 20, verbose: bool = False):

        tf_str = tf.strip().lower()
        symbol = symbol.strip().lower()
        semaphore = asyncio.Semaphore(threads)
        async def get_slug(suffix: int):
            slug = symbol + "-updown-" + tf_str + "-" + str(suffix)
            async with semaphore: return await cls._event_by_slug(session, slug)

        tf = cls._tf_to_secs(tf_str)
        now = Timestamp.utcnow().timestamp()
        start = int(now - lookback * 3600)
        now = int(now - (now % tf))

        batch_size = threads * 3
        suffixes = [*range(now, start, -tf)]
        iterator = range(len(suffixes) // batch_size)
        if verbose: iterator = tqdm.tqdm(iterator)

        for index in iterator:
            start = index * batch_size
            batch = suffixes[start : (start + batch_size)]
            futures = [get_slug(suffix) for suffix in batch]
            results = await asyncio.gather(*futures)
            for event in results:
                if not event: continue
                else: return event

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _map_pmarket(cls, symbols: List[str], polyevents: List[str]):
        async with ClientSession() as session:
            tasks = dict[str, Any]()
            for symbol in symbols:
                for tf in polyevents:
                    task = cls._find_events(session, tf, symbol)
                    tasks[(symbol, tf)] = task
            results = await asyncio.gather(*tasks.values())

        tasks = zip(tasks.keys(), results)
        stoe, etos = dict[str, Any](), dict[str, Any]()

        for (symbol, tf), event in tasks:
            if event is None: continue
            ids = cls._extract_ids(event)
            if ids is None: continue
            stoe[symbol + "+" + tf] = ids["+"]
            stoe[symbol + "-" + tf] = ids["-"]
            etos[ids["+"]] = symbol + "+" + tf
            etos[ids["-"]] = symbol + "-" + tf

        return stoe, etos
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    polyevents = ["1s", "1m", "5m", "15m", "30m", "1h", "4h", "1d"]
    symbols = ["BTC", "ETH", "SOL", "XRP", "DOGE", "DOT", "ADA", "LINK", "BCH", "LTC"]
    stoe, etos = asyncio.run(Connector._map_pmarket(symbols, polyevents))
    print(stoe)
    print(etos)