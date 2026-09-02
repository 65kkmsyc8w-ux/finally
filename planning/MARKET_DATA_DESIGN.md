# Market Data Backend — Detailed Design

The implementation-level design for FinAlly's market data subsystem: the unified
interface every consumer codes against, the GBM simulator that backs the default
zero-config path, and the Massive (Polygon.io) REST poller that backs the real-data path.

This document is meant to be **implementable as written**. Every code block below is
either the intended contents of a module or a directly usable fragment. Field names in
the Massive section were verified against the installed `massive==2.2.0` package, not
taken from documentation.

**Companion documents**

| Document | Covers |
|---|---|
| [MARKET_INTERFACE.md](MARKET_INTERFACE.md) | Why the interface has this shape — rationale, not code |
| [MARKET_SIMULATOR.md](MARKET_SIMULATOR.md) | The simulation model, parameter tuning, statistical properties |
| [MASSIVE_API.md](MASSIVE_API.md) | The Massive API itself — endpoints, plans, object model |
| [MARKET_DATA_SUMMARY.md](MARKET_DATA_SUMMARY.md) | What currently exists in `backend/app/market/` |

**Relationship to the existing code.** `backend/app/market/` already implements most of
this design. Where this document specifies something the current code does not do, the
paragraph is marked **▲ CHANGE** and the full list is collected in
[§15 Deltas from the current implementation](#15-deltas-from-the-current-implementation).

---

## Table of Contents

1. [Scope and responsibilities](#1-scope-and-responsibilities)
2. [Module layout](#2-module-layout)
3. [Data model — `models.py`](#3-data-model--modelspy)
4. [Price cache — `cache.py`](#4-price-cache--cachepy)
5. [The interface — `interface.py`](#5-the-interface--interfacepy)
6. [Simulation parameters — `seed_prices.py`](#6-simulation-parameters--seed_pricespy)
7. [The simulator — `simulator.py`](#7-the-simulator--simulatorpy)
8. [The Massive client — `massive_client.py`](#8-the-massive-client--massive_clientpy)
9. [The factory — `factory.py`](#9-the-factory--factorypy)
10. [SSE streaming — `stream.py`](#10-sse-streaming--streampy)
11. [Application wiring and consumers](#11-application-wiring-and-consumers)
12. [Testing](#12-testing)
13. [Error handling and edge cases](#13-error-handling-and-edge-cases)
14. [Configuration reference](#14-configuration-reference)
15. [Deltas from the current implementation](#15-deltas-from-the-current-implementation)
16. [Extension points](#16-extension-points)

---

## 1. Scope and responsibilities

The subsystem owns exactly one thing: **the latest price of every ticker the application
cares about, kept current, from whichever source is configured.**

| In scope | Out of scope |
|---|---|
| Producing a live price per watched ticker | Deciding *which* tickers are watched (that is the watchlist, in SQLite) |
| Normalising two very different sources into one shape | Historical bars / OHLC charts (see [§16](#16-extension-points)) |
| Holding the current price in memory for cheap reads | Persisting prices — nothing here writes to the database |
| Streaming price changes to the browser over SSE | Portfolio valuation, P&L, trade execution — those *read* from here |
| Adding and removing tickers at runtime | Bid/ask, order books, volume, market hours |

Two invariants hold everything together:

1. **Nothing downstream of the cache can tell which source is running.** No consumer
   branches on `MASSIVE_API_KEY`, imports `MassiveDataSource`, or knows what a poll
   interval is.
2. **The cache is the only channel between producer and consumer.** Sources write; the
   SSE endpoint, portfolio valuation and trade execution read. They never reference each
   other.

---

## 2. Module layout

```
backend/app/market/
├── __init__.py          # Public API surface — the only import path consumers use
├── models.py            # PriceUpdate — the value object
├── cache.py             # PriceCache — thread-safe, versioned, in-memory
├── interface.py         # MarketDataSource ABC + normalize_ticker()
├── seed_prices.py       # Pure data: seed prices, GBM params, correlation groups
├── simulator.py         # GBMSimulator (math) + SimulatorDataSource (plumbing)
├── massive_client.py    # MassiveDataSource — REST poller
├── factory.py           # create_market_data_source() — the one MASSIVE_API_KEY read
└── stream.py            # create_stream_router() — SSE endpoint
```

**Dependency direction is strictly downward:**

```
stream.py ──┐
factory.py ─┼──> simulator.py ──┐
            │    massive_client ┤──> interface.py ──> models.py
            └──> cache.py ──────┴──> models.py
                 seed_prices.py  (no imports at all)
```

`cache.py` never imports a source. `interface.py` never imports an implementation.
`seed_prices.py` imports nothing, so tuning the simulation cannot break anything by
accident.

```python
# app/market/__init__.py
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    normalize_ticker          - Canonical ticker form (uppercase, stripped)
    create_market_data_source - Factory selecting simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource, normalize_ticker
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "normalize_ticker",
    "create_market_data_source",
    "create_stream_router",
]
```

Consumers import from `app.market`, never from `app.market.simulator` or
`app.market.massive_client`. Anything that imports a concrete source outside of
`factory.py` and the tests is a design violation.

---

## 3. Data model — `models.py`

One value object, spoken by every layer from the GBM step to the browser.

```python
# app/market/models.py
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time.

    `previous_price` is the price at the *previous tick*, not the previous
    session's close. It exists to drive the green/red flash animation in the
    watchlist. Day-over-day change belongs to the portfolio layer, which knows
    the reference price.
    """

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute change from the previous tick."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from the previous tick. 0.0 if previous is 0."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' — drives the flash colour."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialise for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

| Decision | Why |
|---|---|
| `frozen=True` | One cached object is handed to many readers concurrently. Immutability means no reader can corrupt another's view, and no defensive copying is needed. |
| `slots=True` | At 10 tickers × 2 Hz this is 20 objects/second forever. Slots cut per-instance memory and speed up attribute access. |
| `change` / `change_percent` / `direction` are properties | Stored copies could drift out of sync with `price`. Derived values are free here — they are computed once per serialisation, not per tick. |
| `timestamp` is Unix **seconds as a float** | Massive speaks nanoseconds, the simulator speaks `time.time()`. Both are normalised at the boundary so no consumer ever has to ask what unit it is holding. |
| No `volume`, `bid`, `ask` | The simulator cannot produce them honestly, so including them would make the two sources visibly different. Market orders fill at `price`; nothing downstream needs more. |

**Precision.** `price` and `previous_price` are rounded to 2 decimals by the cache (§4);
`change` and `change_percent` to 4. A sub-cent GBM move therefore reports as `flat`,
which is correct — the UI should not flash on a change it cannot display.

---

## 4. Price cache — `cache.py`

The single point of truth. Producers write, consumers read, neither knows the other.

```python
# app/market/cache.py
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe store of the latest price for each ticker.

    Writers: exactly one MarketDataSource (simulator or Massive poller).
    Readers: SSE streaming, portfolio valuation, trade execution.

    The cache derives `previous_price` itself, so sources only ever supply a
    new price. Prices are rounded to 2dp on write — this is the single place
    display precision is decided.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonic; bumped on every update

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
    ) -> PriceUpdate:
        """Record a new price. Returns the stored PriceUpdate.

        On the first update for a ticker, previous_price == price, so
        direction == 'flat' and the UI does not flash on page load.
        """
        with self._lock:
            # `if timestamp is None`, not `timestamp or ...`: a genuine 0.0
            # timestamp must not be silently replaced by now().
            ts = time.time() if timestamp is None else timestamp
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest update for one ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of every known price. Shallow copy — safe to iterate."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience accessor for just the price."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Evict a ticker (called when it leaves the watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonic counter, bumped on every update. Drives SSE change detection.

        Read without the lock: an int attribute read is atomic under CPython's
        GIL, and a stale read costs at most one 500ms SSE cycle of latency.
        """
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why `threading.Lock` rather than `asyncio.Lock`

The simulator runs entirely on the event loop, where an `asyncio.Lock` would do. The
Massive poller does not: its `urllib3`-based client is synchronous and runs under
`asyncio.to_thread`, so writes originate on a worker thread. A `threading.Lock` is
correct for both, and the critical sections are two or three dict operations — the
contention cost is nothing next to the correctness it buys.

### Why the version counter

The SSE generator wakes every 500 ms. Without a change signal it would either re-send
the full price map every tick — pointless when Massive only updates every 15 seconds —
or diff the map itself. A monotonic counter turns that into one integer comparison.

The counter is deliberately coarse: *any* write bumps it, so one changed ticker resends
all of them. For a ten-ticker watchlist the whole payload is under a kilobyte, and
per-ticker diffing would cost more than it saves.

### Memory

`O(tickers)`. The cache stores exactly one `PriceUpdate` per ticker and no history — a
long-running container's footprint is flat. Tick history for charts is accumulated by
the *frontend* from the SSE stream, per [PLAN.md](PLAN.md) §2.

---

## 5. The interface — `interface.py`

Five methods, only one of which returns anything.

```python
# app/market/interface.py
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


def normalize_ticker(ticker: str) -> str:
    """Canonical ticker form: uppercase, whitespace stripped.

    Massive's ticker matching is case-sensitive — `aapl` silently returns
    nothing — and a ticker used as a dict key in two different cases would
    produce two independent cache entries. Every entry point normalises:
    source methods, watchlist routes, and trade execution.
    """
    return ticker.strip().upper()


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never asks a source for a price — note the
    absence of any `get_price` here. That is the cache's job, and keeping it
    off this interface is what stops callers coupling to a provider.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates and start the background task.

        Must seed the cache with at least one price per ticker *before*
        returning, so a browser connecting immediately sees a populated
        watchlist. Called exactly once; calling twice is undefined behaviour.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Idempotent, and must never raise — this runs inside FastAPI's lifespan
        shutdown, where an exception produces an ugly traceback on Ctrl-C.
        After stop(), the source never writes to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add to the active set. No-op if already present.

        Eventually consistent: the simulator can seed a price instantly, the
        Massive poller cannot until its next cycle.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove from the active set *and evict from the cache*. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers. Synchronous — it reads in-memory state."""
```

### Contract rules that implementations must honour

| Rule | Why it matters |
|---|---|
| **The cache is injected at construction.** | Inverts the dependency: the cache does not know sources exist, so tests can populate it by hand with no source at all. |
| **`start()` seeds the cache before returning.** | Otherwise a browser connecting in the first 500 ms (simulator) or 15 s (Massive) gets an empty watchlist. `SimulatorDataSource` seeds from `SEED_PRICES`; `MassiveDataSource` awaits one immediate `_poll_once()`. |
| **`add_ticker()` is eventually consistent, and *how* eventually differs.** | Callers must treat "in the watchlist, absent from the cache" as a normal transient state. The watchlist API returns `null` for the price and the UI renders a placeholder. |
| **`remove_ticker()` evicts from the cache.** | `get_all()` returns whatever is in the cache and nothing else prunes it, so a removed ticker would keep appearing in the SSE payload forever. |
| **All tickers are normalised at the source boundary.** | Via `normalize_ticker()`, in `start`, `add_ticker` and `remove_ticker`. **▲ CHANGE** — `SimulatorDataSource` currently does not normalise, so `add_ticker("aapl")` creates a second, lowercase simulation. |
| **`stop()` is idempotent and swallows `CancelledError`.** | The pattern is cancel-then-await-swallowing; see the implementations below. |
| **Background loops never let an exception escape.** | An uncaught exception in an `asyncio.Task` kills it *silently*: prices simply stop, with no error anywhere the user can see. |

---

## 6. Simulation parameters — `seed_prices.py`

Configuration as data. No imports, no logic — tuning the simulation never means touching
code, and tests can import these constants to assert against them.

```python
# app/market/seed_prices.py
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist.
# Plausible-as-of-authoring, not current — they need to be recognisable, not accurate.
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters.
#   sigma: annualised volatility (higher = more movement)
#   mu:    annualised drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},   # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},     # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Applied to any ticker a user adds that is not listed above.
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Price range for seeding an unknown ticker (uniform random).
UNKNOWN_PRICE_RANGE: tuple[float, float] = (50.0, 300.0)

# Correlation groups for the Cholesky decomposition.
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients.
INTRA_TECH_CORR = 0.6     # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3    # Between sectors, and for unknown tickers
TSLA_CORR = 0.3           # TSLA does its own thing
```

Note there is no separate `DEFAULT_CORR`: unknown tickers correlate at `CROSS_GROUP_CORR`
like any other cross-sector pair. An earlier version defined both with the same value,
which read as though they meant different things.

`UNKNOWN_PRICE_RANGE` is new here — the current code inlines `random.uniform(50.0, 300.0)`
in the simulator. Constants belong in this module so tests can import them.

---

## 7. The simulator — `simulator.py`

The default source. It exists so that a student who has cloned the repo and run one
Docker command sees a terminal that *looks alive* — at 2 a.m. on a Sunday, with no API
key. That gives it four requirements, in priority order: always running; visually
convincing at 500 ms; correlated across the watchlist; cheap enough to run forever on
the event loop.

The module splits cleanly in two, and that split is what makes the maths testable:

- **`GBMSimulator`** — the model. Synchronous, no asyncio, no cache, no FastAPI.
  `step()` is a plain function returning a dict, so statistical tests over 100k steps run
  in milliseconds with no event loop and no mocking.
- **`SimulatorDataSource`** — the plumbing. Owns the asyncio task and the cache
  reference, and knows nothing about GBM.

### 7.1 The model

Discretised geometric Brownian motion, one step per tick:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

| Symbol | Meaning |
|---|---|
| `S(t)` | Current price |
| `μ` | Annualised drift |
| `σ` | Annualised volatility |
| `dt` | Time step as a fraction of a **trading** year |
| `Z` | Standard normal draw, correlated across tickers |

GBM is the right model here for three reasons: prices cannot go negative, moves scale
with price level (a $2 move in NVDA at $800 and a $0.50 move in JPM at $195 are the same
*relative* move), and it composes over arbitrary time steps.

The `−σ²/2` term is the Itô correction. Omit it and `E[S(t)] = S(0)·e^(μ+σ²/2)t` — the
simulated drift quietly exceeds the μ you configured. Easy to leave out, hard to notice.

**Choosing `dt`.** A trading year is not a calendar year; prices only move while the
market is open:

```
252 trading days × 6.5 hours × 3600 s = 5,896,800 s
dt = 0.5 / 5,896,800 ≈ 8.479e-8
```

Treating 500 ms of wall clock as 500 ms of *trading* time means an hour of watching the
demo produces roughly an hour of realistic movement. Using calendar seconds (31.5M/year)
would make everything look flat and dead.

The resulting moves:

| Ticker | σ | Price | 1-tick σ | 1-minute σ | 1-hour σ |
|---|---|---|---|---|---|
| AAPL | 0.22 | $190 | $0.012 | $0.13 | $1.03 (0.54%) |
| TSLA | 0.50 | $250 | $0.036 | $0.40 | $3.09 (1.24%) |
| NVDA | 0.40 | $800 | $0.093 | $1.02 | $7.91 (0.99%) |

About a cent per tick on a $190 stock. Enough that the flash animation fires constantly;
small enough that nothing looks unhinged.

**`dt` and `update_interval` are coupled.** `DEFAULT_DT` hard-codes the 500 ms
assumption. Change the interval to 1 s without changing `dt` and every stock's effective
volatility halves. This is the single easiest way to break the simulation's feel.

### 7.2 Correlation via Cholesky

Independent draws per ticker produce a watchlist where AAPL rises while MSFT falls while
GOOGL sits still — visibly wrong to anyone who has watched a real market. This is the
one detail that makes the screen look real.

Build a correlation matrix `C`, factor it as `C = L·Lᵀ`, and transform independent draws:

```python
z = rng.standard_normal(n)   # z ~ N(0, I)
z = cholesky @ z             # Cov(z) = L·Lᵀ = C
```

| Relationship | ρ |
|---|---|
| Tech ↔ tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Finance ↔ finance (JPM, V) | 0.5 |
| TSLA ↔ anything | 0.3 |
| Cross-sector, or any unknown ticker | 0.3 |

TSLA is carved out of the tech block deliberately: it decouples from the sector often
enough in reality that the special case is more realistic than the general rule, and it
gives the watchlist one ticker that visibly does its own thing.

**On positive-definiteness.** `np.linalg.cholesky` raises `LinAlgError` on a
non-positive-definite matrix, and the rebuild happens inside `add_ticker()` — a crash
there is a user-facing failure when someone types a symbol into the watchlist. This
block structure is safe: its minimum eigenvalue is **0.40** (= 1 − 0.6, set by the tech
block) and stays at 0.40 however many tickers are added. There is comfortable margin,
but any future change to these coefficients must re-check the minimum eigenvalue rather
than assume it.

`O(n²)` rebuild per add/remove is irrelevant for a watchlist of tens of tickers.

### 7.3 Shock events

Every tick, every ticker gets a `p = 0.001` chance of a one-off multiplicative jump of
2–5% in a random direction, on top of the GBM step. At 10 tickers and 2 ticks/second that
is ~1.2 events per minute across the watchlist — one visible piece of drama roughly every
50 seconds.

Two consequences are worth stating plainly, because neither is obvious from the
parameters:

1. **Shocks dominate the process.** They contribute ~9.7% hourly standard deviation per
   ticker against ~1% from the diffusion. The carefully tuned per-ticker σ values are, in
   practice, a second-order effect.
2. **Shocks impose a downward drift.** A symmetric multiplicative jump has negative
   expected log return, since `(1+x)(1−x) < 1`. Here `E[log(1+shock)] ≈ −0.000624` per
   event × 7.2 events/hour = **−0.45%/hour**, against μ = 0.05 contributing +0.003%/hour.
   Over a long-lived container, prices trend down.

Neither breaks a demo that lasts minutes with fake money, but both should be a choice
rather than a surprise. If a long-running deployment drifts too far, the fixes in order
of preference are: lower `event_probability` to ~0.0002 (restoring GBM as the dominant
term); make shocks log-symmetric (`exp(±x)` instead of `1±x`) to remove the drag; or add
a slow pull back toward the seed price.

### 7.4 `GBMSimulator`

**▲ CHANGE — injected RNGs.** The current implementation calls
`np.random.standard_normal` and the stdlib `random` module directly, which are two
independent global streams: a test that seeds only one is still non-deterministic. Taking
both generators as constructor arguments makes the whole simulation reproducible from a
seed, which the statistical tests in [§12](#12-testing) rely on.

```python
# app/market/simulator.py  (part 1 of 2)
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
    UNKNOWN_PRICE_RANGE,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Correlated geometric Brownian motion over a set of tickers.

        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Pure synchronous model: no asyncio, no cache, no I/O. Everything that
    touches the event loop lives in SimulatorDataSource.
    """

    # 500ms as a fraction of a trading year:
    # 252 trading days * 6.5 hours * 3600 s = 5,896,800 s
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,
        shock_rng: random.Random | None = None,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        # Two independent streams: numpy for the diffusion, stdlib for shocks
        # and seed prices. Injectable so tests can be fully deterministic.
        self._rng = rng if rng is not None else np.random.default_rng()
        self._shock_rng = shock_rng if shock_rng is not None else random.Random()

        self._tickers: list[str] = []   # Ordered — defines the matrix index mapping
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance every ticker one time step. Returns {ticker: rounded price}.

        The hot path: called twice a second, forever.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # One vectorised draw per tick, not one per ticker.
        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if self._shock_rng.random() < self._event_prob:
                magnitude = self._shock_rng.uniform(0.02, 0.05)
                sign = self._shock_rng.choice((-1, 1))
                self._prices[ticker] *= 1 + magnitude * sign
                logger.debug(
                    "Shock event on %s: %.1f%% %s",
                    ticker,
                    magnitude * 100,
                    "up" if sign > 0 else "down",
                )

            # State keeps full precision; only the emitted value is rounded.
            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker and rebuild the correlation matrix."""
        ticker = normalize_ticker(ticker)
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker and rebuild the correlation matrix."""
        ticker = normalize_ticker(ticker)
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current (unrounded) price, or None if not tracked."""
        return self._prices.get(normalize_ticker(ticker))

    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in matrix order."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — used for batch initialisation."""
        ticker = normalize_ticker(ticker)
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(
            ticker, self._shock_rng.uniform(*UNKNOWN_PRICE_RANGE)
        )
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Refactor the correlation matrix. O(n^2), called only on add/remove."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        # Safe by construction: min eigenvalue of this block structure is 0.40.
        # Re-verify if the correlation constants ever change.
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based correlation between two tickers."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is nominally tech but deliberately decoupled.
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Three choices in `step()` worth keeping:

- **`_prices` holds full precision; only the returned dict is rounded.** Rounding the
  state would accumulate error and could *freeze* a low-priced ticker whose per-tick move
  is under half a cent.
- **One vectorised `standard_normal(n)` per tick**, but a scalar `math.exp` loop. At
  n ≈ 10 the numpy call overhead on the per-ticker maths exceeds the gain, and the scalar
  path is clearer.
- **`add_ticker` rebuilds Cholesky; `_add_ticker_internal` does not.** Construction adds
  all tickers and factors once, instead of factoring n times.

### 7.5 `SimulatorDataSource`

```python
# app/market/simulator.py  (part 2 of 2)


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task that steps the simulation every
    `update_interval` seconds and writes the results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        normalised = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        self._sim = GBMSimulator(
            tickers=normalised,
            event_probability=self._event_prob,
        )

        # Seed the cache synchronously: a browser connecting in the first 500ms
        # must not see an empty watchlist.
        for ticker in normalised:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalised))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass  # Expected: we asked for it.
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if not self._sim:
            return
        self._sim.add_ticker(ticker)
        # Seed immediately so the new ticker has a price before the next tick.
        price = self._sim.get_price(ticker)
        if price is not None:
            self._cache.update(ticker=ticker, price=price)
        logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Step, write, sleep. Forever."""
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                # A raised exception here would kill the task silently and
                # prices would just stop, with nothing visible to the user.
                # Catching costs one tick; not catching costs the demo.
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

`sleep(self._interval)` rather than a deadline-corrected schedule means the cadence
drifts slightly under load. For a visual demo this is invisible and not worth the
complexity — note only that tick count is therefore not a reliable clock.

---

## 8. The Massive client — `massive_client.py`

The optional real-data path, active whenever `MASSIVE_API_KEY` is set.

### 8.1 The constraints that shape it

| Constraint | Consequence |
|---|---|
| Free Basic tier allows **5 calls/minute** | One request per cycle for *all* tickers, never one per ticker. At 15 s that is 4 calls/min, leaving headroom for a retry. |
| Snapshot endpoints require **Starter or above** | On Basic they return `403`. That is terminal, not transient — retrying forever is pure noise. |
| The `RESTClient` is **synchronous** (`urllib3`) | Every call must go through `asyncio.to_thread`, or it blocks the event loop and stalls every SSE stream in the process. |
| The client already retries `413/429/499/5xx` three times | Do **not** add an outer retry loop; it multiplies request count against a 5/minute budget. |
| Data is 15-minute delayed on most paid tiers | Polling faster than ~5 s gains nothing. |
| Markets close | Prices stop changing. The UI must not assume continuous movement. |

### 8.2 Endpoint choice: v3 unified snapshot

**▲ CHANGE.** The current code uses the v2 full-market snapshot
(`get_snapshot_all`). This design specifies the **v3 unified snapshot**
(`list_universal_snapshots`), with v2 retained only as a fallback. Two concrete reasons:

1. `session.price` is populated even when there is no recent trade, so a price is
   available outside market hours without hand-rolling a `prev_day` fallback.
2. Per-ticker `error` / `message` fields mean one bad symbol reports itself instead of
   contaminating the whole response.

```python
snapshots = list(client.list_universal_snapshots(
    type="stocks",
    ticker_any_of=["AAPL", "GOOGL", "MSFT"],
    limit=250,          # NOT optional — see below
))
```

**The `limit` trap.** `list_universal_snapshots` defaults to `limit=10`. A watchlist of
12 tickers would silently return the first ten and the other two would never get a price,
with no error anywhere. Always pass `limit=250` (the maximum, and also the maximum number
of tickers per request).

### 8.3 Verified field names

These were read from the installed `massive==2.2.0` package. Getting them wrong is the
single most likely defect in this module, because a wrong name raises `AttributeError`
that a broad `except` will happily swallow — which is exactly what happened to the
current implementation (see [§15](#15-deltas-from-the-current-implementation)).

`UniversalSnapshot` (v3) — **the complete field list**:

```
ticker, type, session, last_quote, last_trade, greeks, underlying_asset,
details, break_even_price, implied_volatility, open_interest, market_status,
name, fair_market_value, error, message
```

> Note: there is **no** top-level `last_updated` or `timeframe` on `UniversalSnapshot`
> in 2.2.0, despite what MASSIVE_API.md §3 Option B lists. Timestamps come from
> `last_trade`.

`UniversalSnapshotSession`: `price`, `change`, `change_percent`, `open`, `close`, `high`,
`low`, `previous_close`, `volume`, plus early/regular/late trading change breakdowns.
**No `last_updated`, no `vwap`.**

`UniversalSnapshotLastTrade`: `id`, `price`, `size`, `exchange`, `conditions`,
`timeframe`, `last_updated`, `participant_timestamp`, `sip_timestamp`.
Timestamps are Unix **nanoseconds**.

For the v2 fallback path, `TickerSnapshot`: `ticker`, `day`, `prev_day`, `min`,
`last_trade`, `last_quote`, `todays_change`, `todays_change_percent`, `updated`,
`fair_market_value`. Its `LastTrade` has `price`, `size`, `exchange`, `sip_timestamp`,
`participant_timestamp`, `trf_timestamp`, `conditions`, `id`, `sequence_number`, `tape`
— **there is no plain `.timestamp`**.

### 8.4 Extracting a price

A snapshot can be short of any given field. The ladder below is ordered by freshness, and
returns `None` rather than guessing when nothing usable is present — a fabricated price
would corrupt the portfolio permanently.

```python
# app/market/massive_client.py  (part 1 of 2)
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import UniversalSnapshot

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker

logger = logging.getLogger(__name__)

NANOS_PER_SECOND = 1e9

# Substrings identifying a permanently unusable key or plan. BadResponse carries
# only the decoded response body — there is no status code attribute — so this
# is matched against the body text. See is_terminal_error().
TERMINAL_MARKERS = (
    "not_authorized",
    "not entitled",
    "unknown api key",
    "invalid api key",
    "upgrade your plan",
)


def extract_price(snap: UniversalSnapshot) -> tuple[float, float | None] | None:
    """Pull (price, timestamp_seconds) out of a v3 snapshot.

    Returns None when the snapshot carries no usable price at all. Fields are
    accessed explicitly rather than under a broad try/except AttributeError:
    a misspelled field name must fail loudly in tests, not degrade into a
    silent "no data" condition.
    """
    if snap.error:
        logger.warning("Massive error for %s: %s", snap.ticker, snap.message)
        return None

    trade = snap.last_trade
    session = snap.session

    # 1. Last trade — freshest, and carries its own timestamp.
    if trade is not None and trade.price is not None:
        ts = trade.sip_timestamp or trade.participant_timestamp or trade.last_updated
        return float(trade.price), (ts / NANOS_PER_SECOND if ts else None)

    # 2. Session price — populated outside market hours, no timestamp of its own.
    if session is not None and session.price is not None:
        return float(session.price), None

    # 3. Previous close — the honest floor: better a stale real price than none.
    if session is not None and session.previous_close is not None:
        return float(session.previous_close), None

    return None


def is_terminal_error(exc: Exception) -> bool:
    """Classify an API failure as terminal (bad key / unlicensed plan) or transient.

    `BadResponse` exposes only the response body as its message, so the status
    code has to be inferred from the text. 401 bodies say "Unknown API Key";
    403 bodies say "NOT_AUTHORIZED" / "not entitled ... upgrade your plan".
    """
    if isinstance(exc, AuthError):
        return True
    if isinstance(exc, BadResponse):
        body = str(exc).lower()
        return any(marker in body for marker in TERMINAL_MARKERS)
    return False
```

Timestamp handling is the detail to be careful with: `sip_timestamp` is **nanoseconds**,
and `PriceCache` wants **seconds**. Dividing by 1000 instead of 1e9 produces a timestamp
somewhere in the year 54,000 — plausible-looking enough to survive review, and it lands
in the SSE payload where the frontend renders it.

### 8.5 `MassiveDataSource`

```python
# app/market/massive_client.py  (part 2 of 2)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v3/snapshot for every watched ticker in a single request per
    cycle and writes the results to the PriceCache.

    Poll interval guidance:
        Starter / Developer (15-min delayed):  5-15s
        Advanced / Business (real-time):       2-5s
        Basic (free):                          snapshots return 403 — see on_fatal
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        on_fatal: Callable[[str], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._on_fatal = on_fatal
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._degraded = False

    async def start(self, tickers: list[str]) -> None:
        # RESTClient raises AuthError at construction on an empty key, so a
        # missing key fails immediately rather than on the first request.
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))

        # Poll once before returning so the cache is populated for the first
        # SSE connection instead of 15 seconds later.
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added %s (price appears on the next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    async def _poll_loop(self) -> None:
        """Sleep-then-poll: start() already performed the first poll."""
        while not self._degraded:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One cycle: one request, then write every usable price to the cache.

        A failed cycle leaves the cache untouched — stale prices beat an empty
        watchlist — and never raises, so the loop survives to try again.
        """
        tickers = list(self._tickers)  # Snapshot: the worker thread must not
        if not tickers or not self._client:  # see a list mutated mid-request.
            return

        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as exc:
            if is_terminal_error(exc):
                self._enter_degraded(str(exc))
            else:
                # 429, 5xx, timeouts, connection resets. The client already
                # retried; the next cycle will try again.
                logger.warning("Massive poll failed (will retry): %s", exc)
            return

        applied = 0
        for snap in snapshots:
            extracted = extract_price(snap)
            if extracted is None:
                logger.warning("No usable price for %s", snap.ticker)
                continue
            price, timestamp = extracted
            self._cache.update(
                ticker=normalize_ticker(snap.ticker or ""),
                price=price,
                timestamp=timestamp,
            )
            applied += 1

        logger.debug("Massive poll: updated %d/%d tickers", applied, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list[UniversalSnapshot]:
        """Synchronous REST call. Runs in a worker thread, never on the loop.

        limit=250 is mandatory: the endpoint defaults to 10 and would silently
        truncate a larger watchlist.
        """
        assert self._client is not None
        return list(
            self._client.list_universal_snapshots(
                type="stocks",
                ticker_any_of=tickers,
                limit=250,
            )
        )

    def _enter_degraded(self, reason: str) -> None:
        """Stop polling permanently and notify the owner.

        Terminal means the key is wrong or the plan does not license snapshots.
        Retrying every 15 seconds forever would just fill the log.

        on_fatal is scheduled as a separate task rather than awaited: it is
        invoked from inside the poll task, and a handler that calls stop() on
        this source would otherwise cancel and await the very task it is
        running on.
        """
        if self._degraded:
            return
        self._degraded = True
        logger.error("Massive API unusable, stopping poller: %s", reason)
        if self._on_fatal is not None:
            asyncio.create_task(self._on_fatal(reason), name="massive-fatal")
```

### 8.6 Degrading to the simulator

A free-tier key produces a `403` on the first poll, and the honest response is to fall
back to the simulator rather than show a dead watchlist. The fallback is composition, not
inheritance, and it lives in `factory.py` so `MassiveDataSource` stays a plain client:

```python
class MassiveWithSimulatorFallback(MarketDataSource):
    """Massive, degrading to the simulator if the key or plan is unusable.

    Delegates everything to whichever source is currently active, and keeps
    its own ticker list so the replacement starts with the right set.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0):
        self._cache = price_cache
        self._tickers: list[str] = []
        self._active: MarketDataSource = MassiveDataSource(
            api_key=api_key,
            price_cache=price_cache,
            poll_interval=poll_interval,
            on_fatal=self._switch_to_simulator,
        )

    async def start(self, tickers: list[str]) -> None:
        self._tickers = [normalize_ticker(t) for t in tickers]
        await self._active.start(self._tickers)

    async def stop(self) -> None:
        await self._active.stop()

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
        await self._active.add_ticker(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        await self._active.remove_ticker(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _switch_to_simulator(self, reason: str) -> None:
        logger.error(
            "Falling back to the simulator — real market data is unavailable (%s). "
            "Snapshot endpoints require a Starter plan or above.",
            reason,
        )
        await self._active.stop()          # Safe: runs in its own task, and the
        self._active = SimulatorDataSource(price_cache=self._cache)   # poll loop
        await self._active.start(self._tickers)                       # has exited.
```

The switch is loud on purpose. "Why am I seeing fake prices?" is the most likely
confusion in this project, and the answer belongs in the log, at ERROR, with the reason.

---

## 9. The factory — `factory.py`

The only place in the codebase that reads `MASSIVE_API_KEY`.

```python
# app/market/factory.py
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 15.0

# MassiveWithSimulatorFallback (section 8.6) is defined in this module — it is
# the only place that composes the two concrete sources.


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select a market data source from the environment.

        MASSIVE_API_KEY set and non-empty  -> Massive REST poller (real data)
        otherwise                          -> GBM simulator

    Returns an *unstarted* source: construction has no side effects, and the
    caller owns `await source.start(tickers)`. That keeps the factory trivially
    testable and puts startup ordering under the app's control.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", DEFAULT_POLL_INTERVAL))
        logger.info("Market data source: Massive API (real data), %.1fs poll", interval)
        return MassiveWithSimulatorFallback(
            api_key=api_key,
            price_cache=price_cache,
            poll_interval=interval,
        )

    logger.info("Market data source: GBM Simulator (set MASSIVE_API_KEY for real data)")
    return SimulatorDataSource(price_cache=price_cache)
```

Three small decisions carry weight here:

- **`.strip()` before the truthiness check.** `MASSIVE_API_KEY=` and
  `MASSIVE_API_KEY="   "` in a `.env` both mean "not set". Without the strip, a stray
  space routes to a live client that then fails auth.
- **It logs which path was taken, at INFO.** This should be in the first ten lines of
  container output.
- **It returns unstarted.** No network, no tasks, no surprises at import time.

Adding a third provider (Alpaca, Finnhub, IEX) means adding a branch here and
implementing the ABC. Nothing else in the codebase changes.

---

## 10. SSE streaming — `stream.py`

`GET /api/stream/prices` — a long-lived `text/event-stream` the browser consumes with
the native `EventSource` API.

**▲ CHANGE — build the router inside the factory.** The current module creates a
module-level `APIRouter` and registers the route onto it from inside
`create_stream_router()`. Calling that function twice (as tests naturally do) registers
`/prices` twice on the same router object. The router must be constructed per call.

```python
# app/market/stream.py
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

STREAM_INTERVAL = 0.5     # How often to check for changes
HEARTBEAT_INTERVAL = 15.0  # Max silence before sending a comment frame


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build the SSE router bound to a specific PriceCache.

    A factory rather than a module-level router: the cache is injected without
    globals, and calling this twice (in tests) yields two independent routers
    instead of double-registering one route.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """Live price stream. Consume with `new EventSource('/api/stream/prices')`."""
        return StreamingResponse(
            price_event_stream(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Defeat nginx response buffering
            },
        )

    return router


async def price_event_stream(
    price_cache: PriceCache,
    request: Request,
    interval: float = STREAM_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects.

    Sends the full price map whenever the cache version changes, and a comment
    frame during long silences so idle proxies do not close the connection.
    """
    # Browser auto-reconnect delay. EventSource handles reconnection itself.
    yield "retry: 1000\n\n"

    last_version = -1
    last_emit = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    try:
        while True:
            if await request.is_disconnected():
                break

            now = time.monotonic()
            version = price_cache.version

            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                    yield f"data: {payload}\n\n"
                    last_emit = now
            elif now - last_emit >= heartbeat:
                # A comment frame: keeps the socket warm under the Massive
                # source, which changes nothing for 15 seconds at a time.
                yield ": keepalive\n\n"
                last_emit = now

            await asyncio.sleep(interval)
    finally:
        logger.info("SSE client disconnected: %s", client)
```

### Wire format

Frames are unnamed `data:` events, so the browser fires the default `message` event and
`es.onmessage` works without `addEventListener`. The payload is the full price map keyed
by ticker:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.42,"previous_price":190.38,"timestamp":1772452800.13,"change":0.04,"change_percent":0.021,"direction":"up"},"TSLA":{...}}

: keepalive

data: {"AAPL":{...},"TSLA":{...}}
```

Consuming it:

```javascript
const es = new EventSource("/api/stream/prices");

es.onmessage = (event) => {
  const prices = JSON.parse(event.data);   // { AAPL: {...}, TSLA: {...} }
  for (const [ticker, update] of Object.entries(prices)) {
    applyPrice(ticker, update);            // flash on update.direction
  }
};

es.onerror = () => setConnectionStatus("reconnecting");  // EventSource retries itself
es.onopen  = () => setConnectionStatus("connected");
```

### Design notes

- **Poll-and-push, not event-driven.** The generator wakes every 500 ms and compares
  `cache.version`. A pub/sub fan-out from the cache would be more elegant and would add a
  subscriber registry, per-client queues, and backpressure policy — for a ~1 KB payload
  at 2 Hz that is complexity with no payoff. The 500 ms poll also naturally coalesces
  bursts, which is exactly what a UI wants.
- **The full map, every time.** Simpler for the client (no merge logic, no missed-delta
  bugs) and cheap at this size. Per-ticker diffing becomes worth it somewhere north of a
  hundred tickers.
- **Tickers absent from the cache are simply absent from the payload.** The frontend
  renders a placeholder for a watchlist entry with no price — the normal state for a
  ticker just added under the Massive source.
- **Disconnect detection via `request.is_disconnected()`** rather than waiting for a
  write to fail. Without it, a browser that closed its tab leaves a generator ticking
  forever.
- **Multiple concurrent clients are fine.** Each connection gets its own generator and
  its own `last_version`; the cache read is a lock-protected dict copy.

---

## 11. Application wiring and consumers

### 11.1 Lifespan

```python
# app/main.py
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request

from app.db import load_watchlist_tickers, seed_if_empty
from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    seed_if_empty()                              # Lazy DB init (PLAN.md §7)
    cache = PriceCache()
    source = create_market_data_source(cache)

    await source.start(load_watchlist_tickers())  # start() seeds the cache

    app.state.price_cache = cache
    app.state.market_source = source
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(lifespan=lifespan, title="FinAlly")
app.include_router(create_stream_router(app.state.price_cache))
```

State lives on `app.state`, not in module globals, so a test can build an app with a
pre-populated cache and no background task at all.

**Ordering matters:** the database is seeded before the watchlist is read, and
`source.start()` is awaited before `yield` so the app never serves a request against an
empty cache.

For route handlers, a dependency keeps `request.app.state` out of business logic:

```python
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### 11.2 Keeping the watchlist and the source in sync

The watchlist is persisted in SQLite; the source holds its own in-memory ticker set.
Every mutation touches both, **database first**:

```python
@router.post("/api/watchlist")
async def add_watchlist_ticker(
    body: TickerBody,
    cache: PriceCache = Depends(get_price_cache),
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = normalize_ticker(body.ticker)
    db.add_watchlist_ticker(ticker)      # Durable first...
    await source.add_ticker(ticker)      # ...then live.
    # May legitimately be None under Massive until the next poll.
    return {"ticker": ticker, "price": cache.get_price(ticker)}


@router.delete("/api/watchlist/{ticker}")
async def remove_watchlist_ticker(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = normalize_ticker(ticker)
    db.remove_watchlist_ticker(ticker)
    await source.remove_ticker(ticker)   # Also evicts from the cache
    return {"ticker": ticker, "removed": True}
```

Database first means a crash between the two statements leaves a ticker that is persisted
but not streaming — self-healing on the next restart, since `start()` reloads from the
database. The reverse order loses the ticker entirely.

**Removing a ticker with an open position.** The watchlist and the portfolio are separate
concerns, and evicting the price of a held position would make it unpriceable. Two
defensible policies; pick one and state it in the API docs:

1. **Reject** the removal with `409` while a position is open (simplest to reason about).
2. **Keep streaming it** — the source's ticker set becomes the union of the watchlist and
   the tickers with open positions, so `remove_ticker` is only called when both agree.

This design assumes (2), computed in the route layer, because it lets a user tidy their
watchlist without breaking portfolio valuation:

```python
async def remove_if_unheld(ticker: str, source: MarketDataSource) -> None:
    if not db.has_position(ticker):
        await source.remove_ticker(ticker)
```

### 11.3 Rules for consumers

Every consumer must handle a missing price. A ticker can be in the watchlist with no
price: just added under Massive, market closed at first launch, or the symbol does not
exist.

| Consumer | Missing-price behaviour |
|---|---|
| SSE stream | Omit the ticker from the payload; the UI renders a placeholder |
| Watchlist API | Return `"price": null` |
| Portfolio valuation | Value the position at `avg_cost` and flag it stale — never at zero |
| Trade execution | **Reject the trade** |

Trade execution is the one place where guessing is unacceptable — filling an order at a
fabricated price corrupts the portfolio permanently, and the corruption persists in
SQLite long after the price arrives.

```python
@router.post("/api/portfolio/trade")
async def execute_trade(
    body: TradeRequest,
    cache: PriceCache = Depends(get_price_cache),
):
    ticker = normalize_ticker(body.ticker)
    price = cache.get_price(ticker)
    if price is None:
        raise HTTPException(
            status_code=503,
            detail=f"No price available for {ticker}. Try again in a moment.",
        )
    return portfolio.execute(ticker=ticker, side=body.side, quantity=body.quantity, price=price)
```

`503` rather than `400`: the request is well-formed and will succeed shortly. The same
check guards LLM-initiated trades, which run through exactly this path — the resulting
error text is fed back to the model so it can tell the user what happened
([PLAN.md](PLAN.md) §9).

---

## 12. Testing

The interface is the seam that makes everything above testable without a network, a
timer, or a running server.

### 12.1 A fake source is four lines

```python
class FakeDataSource(MarketDataSource):
    def __init__(self, cache): self._cache, self._tickers = cache, []
    async def start(self, tickers): self._tickers = list(tickers)
    async def stop(self): pass
    async def add_ticker(self, t): self._tickers.append(t)
    async def remove_ticker(self, t): self._tickers.remove(t); self._cache.remove(t)
    def get_tickers(self): return list(self._tickers)
```

Portfolio, trade and watchlist tests use this plus a hand-populated `PriceCache`. No
background tasks, no sleeping, fully deterministic.

### 12.2 Conformance tests run against *both* implementations

This is what stops the two sources drifting apart. Parametrise a fixture over both and
assert only contract behaviour:

```python
@pytest.fixture(params=["simulator", "massive"])
async def source(request, cache, monkeypatch):
    if request.param == "simulator":
        src = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    else:
        src = MassiveDataSource(api_key="test", price_cache=cache)
        # RESTClient construction does no I/O, and patching the one synchronous
        # call means nothing reaches the network. _snapshot() is from 12.4.
        monkeypatch.setattr(src, "_fetch_snapshots", lambda tickers: [
            _snapshot(t, 100.0) for t in tickers
        ])
    yield src
    await src.stop()


async def test_start_seeds_cache_before_returning(source, cache):
    await source.start(["AAPL", "MSFT"])
    assert cache.get_price("AAPL") is not None      # not "after a tick" — before return
    assert cache.get_price("MSFT") is not None


async def test_stop_is_idempotent(source):
    await source.start(["AAPL"])
    await source.stop()
    await source.stop()          # must not raise


async def test_remove_evicts_from_cache(source, cache):
    await source.start(["AAPL", "MSFT"])
    await source.remove_ticker("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in source.get_tickers()


async def test_tickers_are_normalised(source):
    await source.start(["aapl", " msft "])
    assert set(source.get_tickers()) == {"AAPL", "MSFT"}


async def test_add_existing_ticker_is_a_noop(source):
    await source.start(["AAPL"])
    await source.add_ticker("AAPL")
    assert source.get_tickers().count("AAPL") == 1
```

### 12.3 The simulator is tested statistically

`GBMSimulator` is synchronous and now takes injected RNGs, so these are fast and
reproducible.

```python
def make_sim(**kwargs):
    return GBMSimulator(
        rng=np.random.default_rng(42),
        shock_rng=random.Random(42),
        **kwargs,
    )


def test_prices_stay_positive():
    sim = make_sim(tickers=["AAPL"])
    for _ in range(100_000):
        sim.step()
    assert sim.get_price("AAPL") > 0


def test_realised_volatility_matches_sigma():
    """With shocks disabled, sample sd of log returns should track sigma*sqrt(dt)."""
    sim = make_sim(tickers=["AAPL"], event_probability=0.0)
    prices = []
    for _ in range(50_000):
        sim.step()
        prices.append(sim.get_price("AAPL"))   # unrounded state: 2dp rounding
    log_returns = np.diff(np.log(prices))      # is ~24% of a one-tick move
    expected = 0.22 * math.sqrt(GBMSimulator.DEFAULT_DT)
    assert log_returns.std() == pytest.approx(expected, rel=0.15)


def test_drift_has_no_sigma_squared_bias():
    """Zero volatility: realised log return must equal mu*dt*steps exactly."""
    sim = GBMSimulator(tickers=["FLAT"], rng=np.random.default_rng(1),
                       shock_rng=random.Random(1), event_probability=0.0)
    sim._params["FLAT"] = {"mu": 0.10, "sigma": 0.0}
    start = sim.get_price("FLAT")
    for _ in range(10_000):
        sim.step()
    expected = start * math.exp(0.10 * GBMSimulator.DEFAULT_DT * 10_000)
    assert sim.get_price("FLAT") == pytest.approx(expected, rel=1e-9)


def test_tech_tickers_are_correlated():
    sim = make_sim(tickers=["AAPL", "MSFT"], event_probability=0.0)
    rows = [sim.step() for _ in range(50_000)]
    a = np.diff(np.log([r["AAPL"] for r in rows]))
    m = np.diff(np.log([r["MSFT"] for r in rows]))
    assert np.corrcoef(a, m)[0, 1] == pytest.approx(0.6, abs=0.1)


def test_cholesky_survives_the_full_default_watchlist():
    sim = make_sim(tickers=list(SEED_PRICES))
    assert len(sim.step()) == 10


def test_cholesky_survives_add_remove_churn():
    sim = make_sim(tickers=["AAPL"])
    for symbol in ["ZZZZ", "TSLA", "JPM", "QQQQ"]:
        sim.add_ticker(symbol)
        sim.step()
    for symbol in ["AAPL", "TSLA"]:
        sim.remove_ticker(symbol)
        sim.step()


def test_unknown_ticker_gets_defaults():
    sim = make_sim(tickers=[])
    sim.add_ticker("ZZZZ")
    low, high = UNKNOWN_PRICE_RANGE
    assert low <= sim.get_price("ZZZZ") <= high
    assert sim._params["ZZZZ"] == DEFAULT_PARAMS
```

`SimulatorDataSource` is tested separately for lifecycle only — `start()` populates the
cache before returning, `stop()` cancels and is idempotent, `remove_ticker()` evicts, and
the loop survives an injected exception:

```python
async def test_loop_survives_a_failing_step(cache, monkeypatch, caplog):
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start(["AAPL"])
    monkeypatch.setattr(source._sim, "step", Mock(side_effect=RuntimeError("boom")))
    await asyncio.sleep(0.05)
    assert not source._task.done()          # still running after several failures
    assert "Simulator step failed" in caplog.text
    await source.stop()
```

### 12.4 Massive tests use real model objects, not bare mocks

**This is the most important testing rule in the module.** A bare `Mock` answers to *any*
attribute, so a test asserting on `snap.last_trade.timestamp` passes happily against a
client that has no such field. That is not hypothetical: it is exactly how the
nanosecond/millisecond defect described in [§15](#15-deltas-from-the-current-implementation)
survived a green test suite.

Build real snapshots from recorded JSON instead:

```python
from massive.rest.models import UniversalSnapshot

def _snapshot(ticker: str, price: float, ts_nanos: int = 1_675_190_399_000_000_000):
    """A real UniversalSnapshot — a wrong field name fails here, loudly."""
    return UniversalSnapshot.from_dict({
        "ticker": ticker,
        "type": "stocks",
        "market_status": "open",
        "session": {"price": price, "change": -4.54, "change_percent": -3.5,
                    "previous_close": price + 4.54},
        "last_trade": {"price": price, "size": 100, "sip_timestamp": ts_nanos},
    })


async def test_poll_writes_prices_to_the_cache(cache):
    source = MassiveDataSource(api_key="test", price_cache=cache)
    source._client = object()
    source._tickers = ["AAPL", "MSFT"]
    with patch.object(source, "_fetch_snapshots",
                      return_value=[_snapshot("AAPL", 190.0), _snapshot("MSFT", 420.0)]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.0
    assert cache.get_price("MSFT") == 420.0


async def test_nanosecond_timestamps_become_seconds(cache):
    source = MassiveDataSource(api_key="test", price_cache=cache)
    source._client = object()
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots",
                      return_value=[_snapshot("AAPL", 190.0, ts_nanos=1_675_190_399_000_000_000)]):
        await source._poll_once()
    assert cache.get("AAPL").timestamp == pytest.approx(1_675_190_399.0)


def test_price_falls_back_to_session_then_previous_close():
    no_trade = UniversalSnapshot.from_dict({
        "ticker": "AAPL", "session": {"price": 188.5, "previous_close": 190.0},
    })
    assert extract_price(no_trade) == (188.5, None)

    closed = UniversalSnapshot.from_dict({
        "ticker": "AAPL", "session": {"previous_close": 190.0},
    })
    assert extract_price(closed) == (190.0, None)

    empty = UniversalSnapshot.from_dict({"ticker": "AAPL"})
    assert extract_price(empty) is None


def test_per_ticker_error_is_reported_not_priced():
    bad = UniversalSnapshot.from_dict({
        "ticker": "ZZZZ", "error": "NOT_FOUND", "message": "Ticker not found.",
    })
    assert extract_price(bad) is None


@pytest.mark.parametrize("body,terminal", [
    ('{"status":"NOT_AUTHORIZED","message":"You are not entitled to this data."}', True),
    ('{"status":"ERROR","message":"Unknown API Key"}', True),
    ('{"status":"ERROR","message":"Too many requests"}', False),
])
def test_error_classification(body, terminal):
    assert is_terminal_error(BadResponse(body)) is terminal


async def test_transient_failure_leaves_the_cache_intact(cache):
    cache.update("AAPL", 190.0)
    source = MassiveDataSource(api_key="test", price_cache=cache)
    source._client = object()
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", side_effect=BadResponse("timeout")):
        await source._poll_once()       # must not raise
    assert cache.get_price("AAPL") == 190.0     # stale beats empty
    assert source._degraded is False


async def test_terminal_failure_stops_polling(cache):
    source = MassiveDataSource(api_key="bad", price_cache=cache)
    source._client = object()
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots",
                      side_effect=BadResponse('{"status":"NOT_AUTHORIZED"}')):
        await source._poll_once()
    assert source._degraded is True
```

None of these tests touch the network. The one thing they cannot catch is Massive
changing its response shape — that is what the recorded-JSON fixtures make cheap to
re-record when it happens.

### 12.5 Cache, factory and stream

```python
def test_cache_is_thread_safe():
    """1000 writes across 10 threads: every write lands, version is consistent."""
    cache = PriceCache()
    def hammer(n):
        for i in range(100):
            cache.update(f"T{n}", 100.0 + i)
    threads = [Thread(target=hammer, args=(n,)) for n in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(cache) == 10
    assert cache.version == 1000


def test_first_update_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.0)
    assert update.previous_price == 190.0 and update.direction == "flat"


def test_explicit_zero_timestamp_is_preserved():
    """Regression: `timestamp or time.time()` would silently replace 0.0."""
    assert PriceCache().update("AAPL", 190.0, timestamp=0.0).timestamp == 0.0


@pytest.mark.parametrize("value,expected", [
    (None, SimulatorDataSource), ("", SimulatorDataSource), ("   ", SimulatorDataSource),
    ("sk-real-key", MassiveWithSimulatorFallback),
])
def test_factory_selects_by_env(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    else:
        monkeypatch.setenv("MASSIVE_API_KEY", value)
    assert isinstance(create_market_data_source(PriceCache()), expected)
```

The SSE generator is testable directly — no ASGI server needed, just a stub request:

```python
class _StubRequest:
    client = SimpleNamespace(host="test")
    def __init__(self, disconnect_after: int): self._n, self._limit = 0, disconnect_after
    async def is_disconnected(self) -> bool:
        self._n += 1
        return self._n > self._limit


async def test_stream_emits_retry_then_prices():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = [f async for f in price_event_stream(cache, _StubRequest(1), interval=0.0)]
    assert frames[0] == "retry: 1000\n\n"
    assert json.loads(frames[1].removeprefix("data: ").strip())["AAPL"]["price"] == 190.0


async def test_stream_is_silent_when_nothing_changes():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = [f async for f in price_event_stream(cache, _StubRequest(5),
                                                  interval=0.0, heartbeat=1e9)]
    assert len([f for f in frames if f.startswith("data:")]) == 1
```

A full end-to-end check (`httpx.AsyncClient` over `ASGITransport`, reading a few frames
off the real endpoint) belongs in the E2E suite described in [PLAN.md](PLAN.md) §12
rather than the unit tests.

---

## 13. Error handling and edge cases

### Failure modes and responses

| Situation | Detection | Response |
|---|---|---|
| Empty watchlist at startup | `tickers == []` | `start()` succeeds with nothing to do; the simulator's `step()` returns `{}`. The UI shows an empty watchlist and adding a ticker starts it streaming. Not an error. |
| Cache miss during a trade | `cache.get_price()` is `None` | `HTTP 503` with a clear message. Never fabricate a fill price. |
| Cache miss during valuation | Same | Value at `avg_cost`, flag stale. Never value at zero. |
| Invalid Massive key (`401`) | `AuthError`, or `BadResponse` containing "Unknown API Key" | Terminal: stop polling, log at ERROR, fall back to the simulator. |
| Unlicensed plan (`403`) | `BadResponse` containing `NOT_AUTHORIZED` / "not entitled" | Terminal: same handling. This is what a free Basic key does. |
| Rate limited (`429`) | `BadResponse` after the client's own retries | Transient: log at WARNING, retry next cycle. If persistent, lengthen `MASSIVE_POLL_INTERVAL`. |
| Massive `5xx` / timeout / DNS | Any other exception | Transient: cache untouched, retry next cycle. |
| Unknown symbol added | Per-ticker `error` on the v3 snapshot | Log at WARNING, skip. The ticker stays in the watchlist with a `null` price. Under the simulator, an unknown symbol is *valid* and gets a random seed price. |
| Market closed | Prices simply stop changing | Nothing to do. `session.price` / `previous_close` keep the watchlist populated; the UI must not assume movement. |
| Exception inside a background loop | — | Caught, logged, loop continues. An escaping exception kills the task silently and prices freeze with no visible error. |
| Cancellation at shutdown | `asyncio.CancelledError` | Swallowed in `stop()`; anywhere else it propagates. Never convert cancellation into a logged error. |
| Two `start()` calls | — | Undefined; the ABC says so. `create_market_data_source` is called once, from `lifespan`. |
| Client disconnects mid-stream | `request.is_disconnected()` | Generator breaks out of its loop and logs. `EventSource` reconnects on its own after `retry: 1000`. |

### Two rules worth stating on their own

**A failed poll must leave the cache untouched, not cleared.** Stale prices are far
better than an empty watchlist, and the frontend has no way to distinguish "cleared" from
"never had data".

**Never catch `AttributeError` as normal control flow.** It converts a field-name mistake
into a silent "no data" condition that runs forever. Check for `None` explicitly on
fields that are genuinely optional, and let a wrong name raise.

---

## 14. Configuration reference

### Environment variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty (after stripping) selects the Massive source; otherwise the simulator. Read in exactly one place: `factory.py`. |
| `MASSIVE_POLL_INTERVAL` | `15.0` | Seconds between Massive polls. Lower it to 2–5 on a real-time plan. |

### Constants

| Constant | Location | Default | Raising it means |
|---|---|---|---|
| `update_interval` | `SimulatorDataSource` | `0.5` s | Slower ticks. **Must be changed together with `dt`** or effective volatility scales with it. |
| `DEFAULT_DT` | `GBMSimulator` | `0.5 / 5_896_800` | Larger moves per tick. Hard-codes the 500 ms assumption. |
| `event_probability` | `SimulatorDataSource` | `0.001` | More shocks. The single most impactful knob on how the demo *feels*. |
| `sigma` / `mu` | `seed_prices.TICKER_PARAMS` | per-ticker | More movement / long-run trend. Both are dominated by shocks at current settings (§7.3). |
| Correlation constants | `seed_prices` | 0.6 / 0.5 / 0.3 | How much the watchlist moves as a bloc. Re-check the minimum eigenvalue if changed. |
| `STREAM_INTERVAL` | `stream.py` | `0.5` s | Fewer SSE frames, laggier flashes. |
| `HEARTBEAT_INTERVAL` | `stream.py` | `15.0` s | Longer silences; risks idle-proxy disconnects. |
| `poll_interval` | `MassiveDataSource` | `15.0` s | Fewer API calls. Below 12 s on a free-tier-shaped budget risks `429`. |

### Dependencies

`numpy` (Cholesky and the normal draws), `massive>=2.2.0` (a core dependency, not an
extra — the factory imports it unconditionally and the code is simpler for it), `fastapi`
for the SSE router. Nothing else.

---

## 15. Deltas from the current implementation

**Status: all items below are implemented.** `backend/app/market/` was rebuilt against
this design — 210 unit tests, 100% statement coverage, `ruff` clean. The table is kept as
the record of what changed and why, since each row is a defect or a design decision worth
not re-introducing. Ordered by severity.

| # | File | Change | Why |
|---|---|---|---|
| 1 | `massive_client.py:103` | `snap.last_trade.timestamp / 1000.0` → nanosecond-aware extraction from `sip_timestamp` | **`LastTrade` has no `.timestamp`.** Every snapshot raises `AttributeError`, the broad `except` at line 110 swallows it, and the poller runs forever writing *nothing* to the cache. The real-data path does not currently work. |
| 2 | `massive_client.py:110` | Remove `except (AttributeError, TypeError)` around field access; use `extract_price()` with explicit `None` checks | That `except` is what hid defect #1. A wrong field name must fail loudly. |
| 3 | `massive_client.py` | Move from v2 `get_snapshot_all` to v3 `list_universal_snapshots(limit=250)` | `session.price` works outside market hours; per-ticker `error` fields isolate bad symbols. Remember `limit=250` — the default of 10 truncates silently. |
| 4 | `massive_client.py` | Classify `401`/`403` as terminal via response-body matching; stop polling and fall back to the simulator | A free-tier key currently retries a guaranteed `403` every 15 seconds forever, showing a permanently empty watchlist with no explanation. Note `BadResponse` carries **no status code** — only the body text. |
| 5 | `simulator.py` | Normalise tickers in `SimulatorDataSource.start/add_ticker/remove_ticker` | `add_ticker("aapl")` currently creates a second, independent lowercase simulation and a duplicate cache entry. `MassiveDataSource` already normalises — the two sources disagree. |
| 6 | `stream.py:17` | Build the `APIRouter` inside `create_stream_router()` instead of at module level | A second call registers `/prices` twice on the same router. Latent, but it bites the first test that builds two apps. |
| 7 | `cache.py:30` | `ts = timestamp or time.time()` → `time.time() if timestamp is None else timestamp` | An explicit `0.0` timestamp is silently replaced. Harmless today, wrong in principle, and awkward to debug if a source ever emits epoch-zero. |
| 8 | `simulator.py` | Inject `np.random.Generator` and `random.Random` | The diffusion and the shocks draw from two independent global streams, so a test that seeds one is still non-deterministic. Blocks reproducible statistical tests and any seeded E2E price path. |
| 9 | `stream.py` | Add the heartbeat comment frame | Under Massive nothing is sent for 15 s at a stretch; idle proxies close such connections. |
| 10 | `seed_prices.py` | Add `UNKNOWN_PRICE_RANGE`; `simulator.py:151` uses it | Inlined magic numbers that tests cannot import. |
| 11 | `factory.py` | Read `MASSIVE_POLL_INTERVAL`; return the fallback wrapper | Poll cadence is currently unconfigurable, and a real-time plan should not be stuck at 15 s. |
| 12 | tests | Replace bare `Mock`s in `test_massive.py` with `UniversalSnapshot.from_dict(...)` fixtures | A bare `Mock` answers to any attribute — which is precisely why defect #1 shipped with a green suite. |

Items 1–4 were what stood between the code and a working real-data path; items 5–7 were
correctness bugs the simulator path happened not to expose. Two names differ slightly from
the drafts above, because the tests need them importable: `MassiveWithSimulatorFallback`
(public, in `factory.py`) and `is_terminal_error()`.

---

## 16. Extension points

**A third provider** (Alpaca, Finnhub, IEX). Implement `MarketDataSource`, add a branch
to the factory. Nothing else changes — that is the whole point of the ABC.

**Massive WebSocket streaming** (paid tiers). A third implementation that pushes on
message receipt instead of on a timer. Because the interface says nothing about polling,
this requires no change above the cache. The `massive` package ships a `WebSocketClient`
alongside `RESTClient`.

**Historical bars for the detail chart.** A genuinely different access pattern —
request/response, not push — so it belongs in a separate method or interface, not bolted
onto `MarketDataSource`. Under Massive it maps to `client.list_aggs(...)`; under the
simulator it would be synthesised from retained tick history. Today the frontend
accumulates its own history from the SSE stream, which is why this is not needed yet.

**Retained tick history in the simulator.** The simulator already generates every tick
and discards all but the latest. A bounded `deque` per ticker would let the detail chart
render history immediately on page load instead of filling in progressively.

**Multi-user.** The cache is keyed by ticker, not by user, so N users watching
overlapping tickers already share one poll. The source's ticker set becomes the union of
all watchlists, and `remove_ticker` needs reference counting so one user's removal does
not break another's stream. No schema change — every table already carries `user_id`.

**Scripted scenarios.** A flash crash or a sector rotation on demand, for demonstrating
how the AI assistant reacts to a moving market. This is a method on `GBMSimulator`
(`apply_shock(ticker, magnitude)`) plus a debug route — the model is already
well-isolated enough to accept it.

**Intraday volatility profile.** Real volatility is U-shaped across a session, high at
open and close. Scaling σ by time-of-day would add realism for a few lines in `step()`.
