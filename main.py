#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
import asyncio
from argparse import ArgumentParser
from typing import Any, Type
from src import *
from strats import *
from src.utils import TimeFrame as TF
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def parse_args(pairs: list[str]):
    params = dict[str, Any]()
    for pair in pairs:
        if "=" not in pair: raise ValueError(
            f"Parameter \"{pair}\" not in \"K=V\" form")
        key, value = pair.split("=", 1)
        try: params[key] = eval(value)
        except: params[key] = value
    return params

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def test(): # ↑↓
    exec = ExecPolymarket()
    await exec.init_client()
    order = Order(price = 0.01, size = 1,
        venue = "Polymarket",  symbol = "BTC↑M5")
    Log.debug("Sending order: %s" % order.__dict__)
    ok, response = await exec.create_order(order)
    if ok: await exec.delete_order(response.UID)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def main():
    parser = ArgumentParser()
    Main: Type[Strategy] = None
    parser.add_argument("strategy", default = "Test", nargs = "?",
            type = str, help = "Strategy to run (default: Test)")
    parser.add_argument("params", nargs = "*", default = list(),
      help = "Additional parameters as K=V pairs. E.g: foo=1 bar=2")

    args = parser.parse_args()
    Main = globals().get(getattr(args, "strategy"), Test)
    params = parse_args(getattr(args, "params"))

    report = f"Launching strategy: \"{Main.__name__}\"\n => Parameters:"
    for K, V in params.items(): report += f"\n    • \"{K}\": {V!r}"
    Log.info(report)

    strategy: Strategy = Main(**params)
    eb = ExecBus(connectors = ExecConnectors)
    db = DataBus(connectors = DataConnectors,
        callbacks = On.callbacks)
    strategy.link(db, eb)

    On.bind(Polymarket)
    On.start_cron()
    try: await db.run()
    except BaseException as EXC:
        Log.exception(EXC); raise
    finally:
        On.stop_cron()
        try: await strategy.on_kill()
        except Exception as EXC:
            Log.exception(EXC)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): asyncio.run(main())