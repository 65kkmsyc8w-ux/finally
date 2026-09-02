# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Use these imports:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, plus properties `change`, `change_percent`, `direction` ("up"/"down"/"flat"), and `to_dict()` for JSON serialization.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None) -> PriceUpdate`
  - `get(ticker) -> PriceUpdate | None`
  - `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`
  - `remove(ticker)`
  - `version` property — monotonic counter, increments on every update (for SSE change detection)

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)` -> `add_ticker()` / `remove_ticker()` -> `stop()`. `start()` seeds the cache before returning; `remove_ticker()` also evicts from the cache; `stop()` is idempotent.

- **`normalize_ticker(ticker)`** — Uppercase + strip. Call it at every entry point (routes, trades); the sources already do.

- **`create_market_data_source(cache)`** — Factory, and the only place `MASSIVE_API_KEY` is read. Returns an *unstarted* source: `MassiveWithSimulatorFallback` when the key is set (it degrades to the simulator if the key or plan is unusable), otherwise `SimulatorDataSource`.

Every consumer must handle a missing price — a ticker can be watched with no price yet. SSE omits it, the watchlist API returns `null`, portfolio valuation uses `avg_cost` and flags it stale, and **trade execution must reject with HTTP 503** rather than invent a fill price.

### SSE Streaming

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)  # Returns FastAPI APIRouter
# Endpoint: GET /api/stream/prices (text/event-stream)
```

### Environment

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty selects real market data; otherwise the simulator |
| `MASSIVE_POLL_INTERVAL` | `15.0` | Seconds between Massive polls |

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests (210)
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```

## Design Docs

`planning/MARKET_DATA_DESIGN.md` is the implementation-level spec for `app/market/` —
module-by-module code, the Massive field names verified against the installed client,
error-handling rules and the testing strategy. Read it before changing anything in there.
