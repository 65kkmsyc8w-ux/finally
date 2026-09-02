# Market Data Interface

The unified Python API FinAlly uses to obtain stock prices, regardless of whether they
come from the Massive REST API or the built-in simulator.

Companion documents: [MASSIVE_API.md](MASSIVE_API.md) for the real-data provider,
[MARKET_SIMULATOR.md](MARKET_SIMULATOR.md) for the simulated one.

---

## 1. The problem this solves

FinAlly needs live prices for two very different reasons:

- **The demo path.** A student clones the repo, runs one Docker command, and sees prices
  move. No API key, no signup, no market hours.
- **The real path.** Someone with a Massive key wants actual market data.

These two sources have almost nothing in common operationally. The simulator produces a
new price for every ticker every 500 ms, in-process, for free. Massive returns polled
snapshots every 15 seconds over the network, subject to rate limits, plan entitlements,
and market hours.

If that difference leaks upward, it contaminates everything: the SSE endpoint would need
to know about poll intervals, trade execution would need to know that a price might be
15 minutes stale, and every test would need a fake HTTP server.

**The design goal is that nothing downstream of the cache can tell which source is
running.**

---

## 2. Shape

```
                    MASSIVE_API_KEY?
                           │
              ┌────────────┴────────────┐
              │ create_market_data_source│      (factory)
              └────────────┬────────────┘
                           │ returns MarketDataSource
         ┌─────────────────┴─────────────────┐
         │                                   │
  SimulatorDataSource                 MassiveDataSource
  (GBM, 500 ms ticks)                 (REST poll, 15 s)
         │                                   │
         └─────────────────┬─────────────────┘
                           │ writes
                    ┌──────▼──────┐
                    │  PriceCache │   thread-safe, in-memory, versioned
                    └──────┬──────┘
                           │ reads
        ┌──────────────────┼──────────────────┐
        │                  │                  │
  SSE /api/stream    Portfolio            Trade
     /prices         valuation           execution
```

Three things carry the whole design:

1. **`MarketDataSource`** — an ABC with a lifecycle, not a getter. Sources *push*.
2. **`PriceCache`** — the single point of truth. Producers write, consumers read, and
   the two never reference each other.
3. **`create_market_data_source`** — the one place in the codebase that reads
   `MASSIVE_API_KEY`.

### Why push, not pull

The obvious interface is `get_price(ticker) -> float`. It is the wrong one here.

Under the simulator, a pull interface would mean either stepping the GBM on demand
(making price history depend on how often someone asks — two readers in the same tick
would see different prices) or stepping in the background anyway and reading state,
which is the cache design with extra steps.

Under Massive, a pull interface would mean one HTTP request per read. With a 5-calls-per-minute
free tier and an SSE stream pushing at 2 Hz, that is over budget by three orders of
magnitude.

Push into a shared cache is the only shape where both sources behave sensibly, and it
lets each one run at its natural cadence.

---

## 3. `PriceUpdate`

The value object every layer speaks. Immutable, so a cached object handed to five
readers cannot be mutated by any of them.

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)   # Unix seconds

    @property
    def change(self) -> float: ...           # price - previous_price
    @property
    def change_percent(self) -> float: ...   # guarded against previous_price == 0
    @property
    def direction(self) -> str: ...          # "up" | "down" | "flat"

    def to_dict(self) -> dict: ...           # for JSON / SSE
```

Design notes:

- **`previous_price` is the previous *tick*, not the previous *close*.** It exists to
  drive the green/red flash animation in the watchlist. Day-over-day change is a
  separate concern and belongs to the portfolio layer, which knows the reference price.
  Conflating the two produces a UI that flashes green all day because the stock is up
  since yesterday.
- **`change` / `change_percent` / `direction` are computed properties, not stored
  fields.** They cannot drift out of sync with `price`.
- **`timestamp` is Unix seconds as a float**, everywhere. Massive's nanoseconds and
  the simulator's `time.time()` are both normalised at the boundary so no consumer ever
  has to ask what unit it is holding.
- **`slots=True`** because there is one of these per ticker per tick — 20 per second
  with 10 tickers, indefinitely.

---

## 4. `MarketDataSource`

```python
class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Starts a background task.
        Called exactly once. Calling twice is undefined behaviour."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.
        Idempotent. After stop(), the source never writes to the cache again."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add to the active set. No-op if present. Effective next cycle."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove from the active set and evict from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Current actively-tracked tickers."""
```

The interface is deliberately small — five methods, and only one of them returns
anything. Note what is *absent*: there is no `get_price`. A source cannot be queried for
a price. That is the cache's job, and keeping it off this interface is what prevents
callers from accidentally coupling to a specific provider.

### Contract details that matter

**Constructor takes the cache.** Every implementation receives the `PriceCache` at
construction and writes to it directly. This inverts the dependency: the cache does not
know sources exist.

**`start()` must seed the cache before returning.** Both implementations write at least
one price per ticker synchronously during `start()`, before spawning their background
task. Without this, a browser connecting in the first 500 ms (simulator) or 15 seconds
(Massive) gets an empty watchlist. `MassiveDataSource` does this with an immediate
`await self._poll_once()`; `SimulatorDataSource` seeds from the initial GBM prices.

**`add_ticker()` is eventually consistent, and how eventual differs.** The simulator can
seed a new ticker's price instantly from `SEED_PRICES`. Massive cannot — the price only
appears after the next poll, up to 15 seconds later. Callers must therefore treat
"ticker present in the watchlist but absent from the cache" as a normal transient state,
not an error. The watchlist API returns `null` for such a price and the UI renders a
placeholder.

**`remove_ticker()` evicts from the cache.** Otherwise a removed ticker keeps appearing
in the SSE payload — `get_all()` returns whatever is in the cache, and nothing else
prunes it.

**Ticker symbols are normalised to uppercase, stripped, at the source boundary.** Massive
ticker matching is case-sensitive; `aapl` silently returns nothing.

**`stop()` is idempotent and must not raise.** It runs during FastAPI's lifespan
shutdown, where a raised exception produces an ugly and confusing traceback on Ctrl-C.
The pattern is cancel-then-await-swallowing-`CancelledError`.

---

## 5. `PriceCache`

```python
class PriceCache:
    def update(self, ticker: str, price: float,
               timestamp: float | None = None) -> PriceUpdate: ...
    def get(self, ticker: str) -> PriceUpdate | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...       # shallow copy
    def get_price(self, ticker: str) -> float | None: ...
    def remove(self, ticker: str) -> None: ...

    @property
    def version(self) -> int: ...                          # bumped on every update

    def __len__(self) -> int: ...
    def __contains__(self, ticker: str) -> bool: ...
```

### Why `threading.Lock` and not an asyncio lock

The simulator runs entirely on the event loop, so an `asyncio.Lock` would suffice for it.
The Massive poller does not: its `urllib3` client is synchronous and runs under
`asyncio.to_thread`, so writes originate on a worker thread. A `threading.Lock` is
correct for both, and the critical sections are a few dict operations — the contention
cost is irrelevant next to correctness.

### Why the version counter

The SSE generator wakes every 500 ms. Without a change signal it would either re-send
the full price map every tick — pointless traffic when Massive only updates every 15
seconds — or diff the whole map itself.

A monotonic counter bumped on every `update()` makes this a single integer comparison:

```python
if cache.version != last_version:
    last_version = cache.version
    yield f"data: {json.dumps(...)}\n\n"
```

The counter is coarse — any write bumps it, so one changed ticker resends all of them.
That is the right trade for a ten-ticker watchlist, where the whole payload is under a
kilobyte and per-ticker diffing would cost more than it saves.

Note that `version` is read outside the lock. On CPython an `int` attribute read is
atomic, and a stale read costs at most one 500 ms cycle of latency.

### `update()` derives `previous_price` itself

Callers pass only the new price. The cache looks up the prior `PriceUpdate` and fills in
`previous_price`, so neither source has to track it. On the first update for a ticker,
`previous_price == price`, giving `direction == "flat"` — no spurious flash on page load.

Prices are rounded to 2 decimal places on write, so the cache is the single place where
display precision is decided.

---

## 6. The factory

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

Small, but it carries three decisions:

- **`.strip()` before the truthiness check.** `MASSIVE_API_KEY=` and
  `MASSIVE_API_KEY="   "` in a `.env` file both mean "not set". Without the strip, a
  stray space silently routes to a live client that then fails auth.
- **It returns an *unstarted* source.** Construction has no side effects; the caller
  owns `await source.start(tickers)`. This keeps the factory trivially testable and puts
  startup ordering under the app's control.
- **It logs which path was taken, at INFO.** "Why am I seeing fake prices?" is the single
  most likely confusion in this project, and the answer should be in the first ten lines
  of container output.

This is the **only** place `MASSIVE_API_KEY` is read. No other module branches on the
data source, and adding a third provider means adding a branch here and nothing else.

---

## 7. Wiring into FastAPI

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = PriceCache()
    source = create_market_data_source(cache)

    tickers = load_watchlist_tickers()          # from SQLite; seeded on first run
    await source.start(tickers)

    app.state.price_cache = cache
    app.state.market_source = source
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(app.state.price_cache))
```

State lives on `app.state`, not in module globals, so tests can build an app with a
pre-populated cache and no background task at all.

### Keeping the watchlist and the source in sync

The watchlist is persisted in SQLite; the source holds its own in-memory ticker set.
Every mutation must touch both, database first:

```python
@router.post("/api/watchlist")
async def add_ticker(request: Request, body: TickerBody):
    ticker = body.ticker.upper().strip()
    db.add_watchlist_ticker(ticker)                     # durable
    await request.app.state.market_source.add_ticker(ticker)   # live
    return {"ticker": ticker, "price": request.app.state.price_cache.get_price(ticker)}
```

Database first means a crash between the two leaves a ticker that is persisted but not
streaming — self-healing on the next restart, since `start()` reloads from the database.
The reverse order loses the ticker entirely.

`price` may legitimately be `null` here under Massive, per §4.

---

## 8. Consuming prices

```python
from app.market import PriceCache, create_market_data_source

cache.get("AAPL")        # PriceUpdate | None
cache.get_price("AAPL")  # float | None
cache.get_all()          # dict[str, PriceUpdate] — snapshot copy
```

Every consumer must handle `None`. A ticker can be in the watchlist without a price
(just added, market closed on first launch, Massive returned no trade). The rules:

| Consumer | On missing price |
|---|---|
| SSE stream | Omit the ticker from the payload |
| Portfolio valuation | Value the position at `avg_cost`, flag it as stale |
| Trade execution | **Reject the trade** with a clear error |

Trade execution is the one place where guessing is unacceptable — filling an order at a
fabricated price corrupts the portfolio permanently. `HTTP 503, "No price available for
AAPL"` is the correct answer.

---

## 9. Testing

The interface is the seam that makes the rest of the system testable.

**A fake source is four lines**, because the ABC is small:

```python
class FakeDataSource(MarketDataSource):
    def __init__(self, cache): self._cache, self._tickers = cache, []
    async def start(self, tickers): self._tickers = list(tickers)
    async def stop(self): pass
    async def add_ticker(self, t): self._tickers.append(t)
    async def remove_ticker(self, t): self._tickers.remove(t); self._cache.remove(t)
    def get_tickers(self): return list(self._tickers)
```

Portfolio, trade, and SSE tests use this plus a hand-populated `PriceCache`. No timers,
no network, no background tasks, fully deterministic.

**Conformance tests run against both real implementations** via a parametrised fixture —
`start()` populates the cache, `stop()` is idempotent, `remove_ticker()` evicts,
`add_ticker()` is a no-op when the ticker is already present, symbols are uppercased.
This is what stops the two implementations from drifting apart.

**The factory is tested by monkeypatching the environment**: unset, empty, whitespace,
and set, asserting the returned type each time.

**Massive tests must use `Mock(spec=...)`, not bare `Mock`.** A bare `Mock` returns a
child mock for *any* attribute, so a test asserting on `snap.last_trade.timestamp`
passes against a client that has no such field. This is not hypothetical — it is exactly
how the field-name defect described in [MASSIVE_API.md](MASSIVE_API.md#known-defect-in-the-current-client)
survived a passing test suite. Specced mocks, or real `TickerSnapshot` objects built
from recorded JSON, turn that into a failure.

---

## 10. Extending it

**A third provider** (Alpaca, Finnhub, IEX): implement the ABC, add a branch to the
factory. Nothing else changes.

**Multi-user**: the cache is already keyed by ticker, not by user, so N users watching
overlapping tickers share one poll. The source's ticker set becomes the union of all
watchlists; `remove_ticker` needs reference counting so one user's removal does not
break another's stream.

**Historical bars for the detail chart**: this is a genuinely different access pattern —
request/response, not push — and should be a separate method or a separate interface,
not bolted onto `MarketDataSource`. Under the simulator it would be synthesised from
retained tick history; under Massive it maps to `list_aggs`.

**WebSocket streaming from Massive** (paid tiers): a third implementation of the same
ABC that pushes on message receipt instead of on a timer. Because the interface says
nothing about polling, this requires no changes above the cache.
