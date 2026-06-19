# Polybinance

A lightweight, standalone trading engine for streaming market data over WebSockets and placing orders across exchanges. The runtime is a single Python process built on `asyncio`: connectors feed a shared in-memory data bundle, and strategies react through event listeners and scheduled tasks.

The first strategies in this repo focus on **cross-venue studies** — consuming **Binance** (liquid, low-latency futures/perp prices) and **Polymarket** (event-contract order books) at the same time. That pairing is deliberate: Binance gives a continuous reference price stream; Polymarket gives structured short-horizon prediction markets. The connector layer is already extended beyond those two — **Bybit** and **Rofex** implementations exist and can be enabled without changing the strategy API.

---

## Quick start

### 1. Install dependencies

```bash
cd /srv/shared/polybinance
pip install -r requirements.txt
```

### 2. Configure credentials

Copy the example config and fill in the sections you need:

```bash
cp config.ini.example config.ini
```

See [Credentials](#credentials) below for section layout.

### 3. Run a strategy

```bash
python main.py                                           # runs Test strategy (default)
python main.py MySuperStrategy                           # runs strat with default values for all parameters
python main.py MySuperStrategy freq=TF.M1 ema_period=14  # runs strat with specific values (rest stays default)
```

The process blocks until interrupted (`Ctrl+C`) or a fatal error. On shutdown, `on_kill` runs for cleanup and exports.

### 4. Check outputs

| Output | Location |
|---|---|
| Runtime logs | `logs/YYYYMMDD_HHmm.log` (UTC timestamp in filename) |
| Order history CSV | `logs/YYYYMMDD_HHMMSS_<Strategy>_history.csv` |
| Custom exports | whatever your `on_kill` writes (see `Test`) |

---

## Running the system

### Command-line interface

`main.py` accepts two positional argument groups:

```
python main.py [strategy] [params...]
```

| Argument | Default | Description |
|---|---|---|
| `strategy` | `Test` | Strategy class name (must be importable from `strats`) |
| `params` | _(none)_ | Zero or more `K=V` pairs overriding dataclass defaults |

Examples:

```bash
python main.py
python main.py Test
python main.py Test freq=TF.H1 # <= You can write timeframes like this
python main.py Test foo=1 bar="hello"
```

### How parameters are parsed

1. `argparse` reads `strategy` and collects remaining tokens as `params`.
2. The strategy class is resolved by name: `globals().get(strategy_name, Test)`.
3. Defaults come from the strategy dataclass via `Strategy.defaults()` (every field with a default value).
4. CLI pairs are merged on top through `parse_args()`:

```python
# main.py — each param must be K=V; values are eval()'d, falling back to raw strings
for pair in pairs:
    key, value = pair.split("=", 1)
    try:    params[key] = eval(value)
    except: params[key] = value
```

Because values pass through `eval`, you can pass Python objects directly:

```bash
python main.py Test freq=TimeFrame.M1
python main.py Test threshold=0.05
```

Strings that are not valid Python expressions are kept as plain strings.

### Systemd (optional)

A unit file template is included (`trading_polybinance-pedro.service`). Point `ExecStart` at your venv Python and strategy name:

```ini
WorkingDirectory=/srv/shared/polybinance
ExecStart=/path/to/venv/bin/python main.py Test
```

Just keep in mind

---

## Credentials

All secrets live in **`config.ini`** at the project root (git-ignored). `src/utils.py` loads it at import time via `ConfigParser`.

| Section | Used by | Keys |
|---|---|---|
| `[BINANCE]` | Binance connectors | `api_key`, `secret` |
| `[BYBIT]` | Bybit connectors | `api_key`, `secret` |
| `[POLYMARKET]` | Polymarket connector | `private_key`, `relayer_key`, `wallet_address`, `relayer_address` |
| `[ROFEX]` | Rofex connector | `username`, `password`, `account`, `symbols` |
| `[PREDEFINED]` | Global settings | `symbols` (space-separated base assets), `polyevents` (Polymarket crypto-event durations) |

Each connector reads its section at class definition time, e.g. `Binance.AUTH = Auth(**CONFIG["BINANCE"])`.

**Adding a new exchange:**

1. Implement `Exchange`, `Data<Name>`, and `Exec<Name>` under `src/connectors/`.
2. Add a `[YOUR_EXCHANGE]` section to `config.ini.example` (and your local `config.ini`).
3. Register the exchange in `src/connectors/__init__.py` — add it to the `exchanges` list (other venues are already present but commented out).

---

## Writing a strategy

Strategies subclass `Strategy` (a `@dataclass`) and live in `strats/`. Register new ones in `strats/__init__.py` so `main.py` can resolve them by name.

The reference implementation is **`Test`** (`strats/test.py`):

```python
@dataclass
class Test(Strategy):
    freq: TimeFrame = TimeFrame.H1          # strategy parameter (dataclass field)

    def setup(self):
        self.last = Timestamp.utcnow()
        self.add_cron(self.test, TimeFrame.S5)  # register a cron task

    @On.tick
    async def on_tick(self, tick: Tick):
        self.last = tick.time                 # runs on every incoming tick

    async def test(self):
        Log.debug("Testing cron...")          # runs every S5

    async def on_kill(self):
        ...                                   # cleanup + CSV export on shutdown
```

### Lifecycle

```
main()
  → Strategy(**params)          # dataclass init → Strategy.__init__
      → setup()                 # your initialization
      → On.bind(self)           # registers cron + tick handlers
  → On.bind(Polymarket)         # connector-level cron (market ID refresh)
  → On.start_cron()
  → await ExecBus.start()       # execution connectors
  → await DataBus.start()       # data connectors (blocks here)
  → finally: on_kill()
```

### Strategy parameters

Declare them as **dataclass fields with defaults** on the strategy class. They become constructor kwargs and CLI-overridable keys:

```python
@dataclass
class MyStrategy(Strategy):
    threshold: float = 0.02
    symbols: str = "BTC"
```

Override at launch: `python main.py MyStrategy threshold=0.05`.

`Strategy.defaults()` collects all fields that have a default and seeds the parameter dict before CLI overrides are applied.

### `setup()`

Called once during `Strategy.__init__`, after internal state (`_orders_current`, `_orders_history`, buses) is created but before connectors start. Use it to:

- Initialize instance attributes
- Register cron tasks with `self.add_cron(method, TimeFrame.X)`
- Do whatever you want at the start of the strategy (even trading)

### Cron tasks (`add_cron`)

Cron methods are **async functions** scheduled on fixed UTC-aligned boundaries (e.g. every `S5`, `M1`, `H1`). Register them in `setup`:

```python
self.add_cron(self.rebalance, TimeFrame.M1)
```

`On.bind` collects every entry in `self.cron` and `On.start_cron` launches one `asyncio` task per method. Each task sleeps until the next boundary, calls the method, and repeats.

Connector classes can also expose a class-level `cron` dict. `main.py` calls `On.bind(Polymarket)` so Polymarket market IDs refresh every `M5` independently of your strategy.

### Tick handlers (`@On.tick`)

Decorate an **async** method with `@On.tick` to subscribe to the global tick callback list. Every data connector pushes parsed `Tick` objects into `Bundle` and then invokes all registered callbacks:

```python
@On.tick
async def on_tick(self, tick: Tick):
    if tick.venue == "BinanceUsdm" and tick.symbol == "BTC":
        ...
```

Only methods marked with `@On.tick` are collected during `On.bind`. Name the method anything you like (`on_tick` is convention).

### `on_kill()`

Async hook called in the `finally` block when the process shuts down (interrupt, crash after buses started, etc.). Use it for:

- Flushing custom datasets to CSV
- Cancelling open orders
- Final logging

The framework also maintains `_orders_history` throughout the session. The `Strategy._on_kill` helper (wraps `on_kill` then writes order history) is available if you call it from your implementation; order records include every create/modify/delete event keyed by UID.

---

## Tools available to strategies

After `strategy.link(db, eb)` in `main.py`, the strategy can access data and execution buses.

### Querying data — `get_data()`

```python
df = self.get_data()                              # all candles, all venues/symbols
df = self.get_data(Tick)                          # tick-level data
df = self.get_data(TimeFrame.M5)                  # one candle timeframe
df = self.get_data({TimeFrame.M1, TimeFrame.H1})  # multiple timeframes

df = self.get_data(
    symbols = {"BinanceUsdm": {"BTC"}, "Polymarket": {"BTC↑M5"}},
    since   = Timestamp("2026-01-01", tz = "UTC"),
    until   = Timestamp.utcnow(),
    n       = 500,                                # max candles per series
)
```

| Parameter | Role |
|---|---|
| `tf` | `Tick` for ticks; a `TimeFrame` or set of them for candles; `None` → all candle timeframes |
| `symbols` | `{venue: {symbol, ...}, ...}` filter; `None` → all connected venues (empty set = all symbols seen so far) |
| `until` | Upper time bound |
| `since` | Lower time bound (defaults to first tick time) |
| `n` | Max candles per series (default: bundle capacity) |

Data is served from each connector's in-memory `Bundle` — ticks are stored per-minute buckets and resampled into candles from `S1` through `D1`. Returns a `pandas.DataFrame` indexed by `(venue, symbol, tf, time)` for candles or `(venue, symbol, time)` for ticks.

### Orders

**Create** an `Order`, then send it through the strategy helper (routes to the correct `ExecConnector` by `order.venue`):

```python
order = Order(
    venue  = "BinanceUsdm",
    symbol = "BTCUSDT",
    size   = -0.0001,     # positive = buy, negative = sell
    price  = 62527.0,     # omit for market-style IOC
)
ok, response = await self.create_order(order)
```

**UID** — assigned at construction from the order timestamp: microsecond epoch encoded in base-36 uppercase. It is the 10-character local identifier before and after the exchange acknowledges the order. 

**Exchange ID (`EID`)** — returned by the venue in the `Response` and stored alongside the UID.

**Storage:**

| Dict | Contents |
|---|---|
| `_orders_current` | Active orders (removed on successful delete) |
| `_orders_history` | All orders ever placed, including deleted (status updated to `"deleted"`) |

**Query / manage:**

```python
self.orders()              # DataFrame of open orders
self.orders("HJEDSJUZJZ")  # single order dict by UID
await self.modify_order(uid, new_order) # Warning: not implemented yet for most exchanges
await self.delete_order(uid)
```

---

## Outputs

### Logs

`loguru` writes to stdout and to `logs/{UTC timestamp}.log`. Level is `DEBUG` by default. Connector subscription changes, cron starts, order results, and exceptions all land here.

### CSV exports

On shutdown, strategies typically persist state in `on_kill`:

- **`Test`** dumps all candles and all ticks collected during the session:
  ```
  logs/{start}-{end}_candles.csv
  logs/{start}-{end}_ticks.csv
  ```
- Order history (when using the `_on_kill` wrapper) would be written as:
  ```
  logs/{start}_{Strategy}_history.csv
  ```

---

## Architecture

### Repository layout

```
polybinance/
├── main.py                 # entry point, argparse, bus wiring
├── config.ini              # credentials (local, git-ignored)
├── config.ini.example
├── requirements.txt
├── strats/
│   ├── __init__.py         # exports strategy classes by name
│   └── test.py             # reference strategy
├── src/
│   ├── strategy.py         # Strategy, On (cron + tick registry)
│   ├── models.py           # Order, Tick, Candle, Bundle
│   ├── utils.py            # Log, TimeFrame, CONFIG loader
│   └── connectors/
│       ├── base.py         # DataStream, DataBus, ExecBus, buses
│       ├── binance.py
│       ├── polymarket.py
│       ├── bybit.py        # optional
│       └── rofex.py        # optional
├── logs/                   # runtime output (git-ignored)
└── tests/
```

### Event-driven concurrency

Everything runs in one `asyncio` event loop. Parallelism comes from **tasks**, not threads:

```
┌─────────────────────────────────────────────────────────────┐
│                        asyncio event loop                   │
├──────────────┬──────────────┬───────────────┬───────────────┤
│ DataBus      │ ExecBus      │ Cron tasks    │ WS callbacks  │
│ (per venue)  │ (per venue)  │ (On.schedule) │ (on_message)  │
└──────────────┴──────────────┴───────────────┴───────────────┘
```

Each `DataConnector` spawns, per WebSocket channel:

- **`start_streams`** — maintains the connection, reads frames
- **`start_channel`** — manages subscribe/unsubscribe from `on_channel`
- **`on_freq`** — resamples ticks → candles on a timer

When a WebSocket frame arrives, `DataStream.read` dispatches it without blocking the read loop:

```python
asyncio.create_task(self.on_message(message.json()))
```

The connector's `on_message_*` handler parses venue-specific JSON into `Tick` / `Candle`, appends to `Bundle`, then fans out to every callback in `On.callbacks` (your `@On.tick` methods).

### Task model summary

| Mechanism | Registration | Trigger | Nature |
|---|---|---|---|
| `@On.tick` | decorate method; collected by `On.bind(self)` | every `Tick` from any connector | event-driven, async |
| `add_cron` | `self.add_cron(fn, TimeFrame.X)` in `setup` | UTC-aligned boundaries | synchronous schedule, async method |
| Connector `cron` | class-level `cron = {method: TimeFrame}` | same scheduler via `On.bind(Connector)` | infrastructure, not strategy |

```
  WebSocket                    Cron scheduler
      │                              │
      ▼                              ▼
 on_message ──► Bundle ──► @On.tick   add_cron ──► scheduled async fn
                  │                        ▲
                  └──── get_data() ◄───────┘
```

---

## Enabling more exchanges

Active connectors are listed in `src/connectors/__init__.py`:

```python
exchanges = [BinanceUsdm, Polymarket]
# BinanceSpot, BinanceCoin, BybitLinear, BybitSpot, BybitInverse, Rofex
```

Uncomment entries to add them to `DataConnectors` / `ExecConnectors`. Each exchange module follows the same shape:

- **`Exchange`** — auth, symbol mapping, status translation
- **`Data<Exchange>`** — WebSocket streams → `Bundle` + callbacks
- **`Exec<Exchange>`** — REST (or SDK) order placement

Strategies refer to venues by the class name (`"BinanceUsdm"`, `"Polymarket"`, `"BybitLinear"`, `"Rofex"`). Both ticks, candles and orders have the "venue" and "symbol" fields for better identification of the source/destination of them.

---

## Roadmap — toward a distributed engine

This repo intentionally keeps everything in one process: connectors, in-memory bundle, and strategy share the same address space. That keeps latency and complexity low for research and small live runs. All in all, this project should be considered as a simple prototype for strategies that don't require extreme performances, to be used by myself for my own personal trading "in the meantime" on my local Windows machine.

This may serve as a starting point for a project I am developing nowadays, which is the natural evolution: **splitting the monolith into independent services**. Everything connected by databases, message brokers and other containers within a cloud server:

At a high level:

- **Data connectors** become standalone processes for each data source (exchanges, trading venues, external dashboards) that normalize ticks/candles and publish to a stream bus with an internal cache (e.g.: Redis).
- A separate **data resampler** collects the data from all of the connectors in the stream bus, creates candles of other timeframes and publishes them back in the stream bus, as well as storing them in a time-serialized database (e.g.: TimescaleDB)
- An **order manager** owns order state and risk management across accounts in a transactional store, and deposits orders in the bus to be collected by **execution connectors** associated to each trading venue fpr further execution. The order responses are placed back in the bus for strategies and other services for consumption and further actions.
- **Strategy management** handles deployment, parameters, and lifecycle separately from **strategy runtime** (the event-loop that consumes market data and emits intents).
- **Monitoring** attaches to the same stores for dashboards, alerts, and post-trade analysis.

The goal is a generalized, high-performance trading stack with real-time storage — same conceptual model as here (`Tick` → listener → `Order`), but with persistence and horizontal scaling between components instead of a single `Bundle` in RAM. Right now being coded in Python, but will be redesigned in C++ in the future (PyBind 11 framework will still allow Python strategies to run with the rest of the C++ ecosystem).

Anyway: COMING SOON :) Thanks for reading!
