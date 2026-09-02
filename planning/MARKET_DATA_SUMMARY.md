# Market Data Backend — Summary

**Status:** Complete. 210 unit tests, 100% statement coverage, `ruff` clean.

The implementation-level specification is [MARKET_DATA_DESIGN.md](MARKET_DATA_DESIGN.md);
rationale lives in [MARKET_INTERFACE.md](MARKET_INTERFACE.md),
[MARKET_SIMULATOR.md](MARKET_SIMULATOR.md) and [MASSIVE_API.md](MASSIVE_API.md).

## What Was Built

A market data subsystem in `backend/app/market/` (9 modules) providing live prices from
either a GBM simulator or the Massive REST API behind one interface.

```
MarketDataSource (ABC)
├── SimulatorDataSource            GBM simulator (default, no API key needed)
└── MassiveWithSimulatorFallback   Massive v3 snapshot poller (MASSIVE_API_KEY set),
    └── MassiveDataSource          degrading to the simulator if the plan/key is unusable
            │
            ▼
       PriceCache (thread-safe, versioned, in-memory)
            │
            ├──> SSE stream endpoint (/api/stream/prices)
            ├──> Portfolio valuation
            └──> Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — frozen, slotted dataclass (ticker, price, previous_price, timestamp + derived change/percent/direction) |
| `interface.py` | `MarketDataSource` ABC (`start`/`stop`/`add_ticker`/`remove_ticker`/`get_tickers`) and `normalize_ticker()` |
| `cache.py` | `PriceCache` — thread-safe store with a version counter for SSE change detection |
| `seed_prices.py` | Seed prices, per-ticker GBM params, correlation groups — pure data, no imports |
| `simulator.py` | `GBMSimulator` (correlated GBM, injected RNGs) + `SimulatorDataSource` (asyncio plumbing) |
| `massive_client.py` | `MassiveDataSource` — v3 unified-snapshot poller, `extract_price()`, `is_terminal_error()` |
| `factory.py` | `create_market_data_source()` + `MassiveWithSimulatorFallback` |
| `stream.py` | `create_stream_router()` and `price_event_stream()` — SSE endpoint |

### Key Design Decisions

- **Push into a shared cache, never pull.** No `get_price` on the source interface, so
  nothing downstream can couple to a provider or trigger an API call per read.
- **One request per poll cycle** covering the whole watchlist — what keeps the Massive
  path inside a 5-calls-per-minute free-tier budget.
- **v3 unified snapshot** (`limit=250`, mandatory — it defaults to 10): `session.price`
  survives market close, and per-ticker `error` fields isolate bad symbols.
- **Terminal vs transient errors.** 401/403 stop the poller and fall back to the
  simulator, loudly; everything else retries next cycle leaving the cache untouched.
- **Correlated GBM via Cholesky** — tech 0.6, finance 0.5, TSLA and cross-sector 0.3 —
  plus ~0.1%/tick shock events for drama.
- **Injected RNGs** (numpy for diffusion, stdlib for shocks) so price paths are
  reproducible from a seed.

## Test Suite

**210 tests, 100% statement coverage** across 9 modules in `backend/tests/market/`.

| Module | Tests | Focus |
|--------|-------|-------|
| test_models.py | 12 | Immutability, slots, derived values, SSE payload shape |
| test_cache.py | 17 | Rounding, previous-price derivation, versioning, concurrent writers |
| test_interface.py | 10 | `normalize_ticker`, ABC enforcement, the four-line fake source |
| test_simulator.py | 35 | GBM statistics: realised vol, Itô drift, correlation, shock bounds, Cholesky robustness |
| test_simulator_source.py | 14 | Lifecycle, cache seeding, loop fault tolerance |
| test_massive.py | 36 | Field extraction, ns→s conversion, error classification, poll behaviour |
| test_stream.py | 14 | Frame format, change detection, heartbeat, disconnect, router wiring |
| test_factory.py | 21 | Env selection, poll interval parsing, fallback switching |
| test_conformance.py | 51 | The same contract asserted against all three implementations |

`test_conformance.py` is what stops the two sources drifting apart: 17 contract tests
parametrised over `SimulatorDataSource`, `MassiveDataSource` and the fallback wrapper.

Massive tests build real `UniversalSnapshot` objects with `from_dict()` rather than
`Mock` — a bare `Mock` answers to any attribute, which is how a wrong field name once
passed a green suite.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source, create_stream_router

# Startup (FastAPI lifespan)
cache = PriceCache()
source = create_market_data_source(cache)   # reads MASSIVE_API_KEY
await source.start(load_watchlist_tickers())  # seeds the cache before returning
app.include_router(create_stream_router(cache))

# Read prices — every consumer must handle None
cache.get("AAPL")        # PriceUpdate | None
cache.get_price("AAPL")  # float | None
cache.get_all()          # dict[str, PriceUpdate] (snapshot copy)

# Dynamic watchlist — database first, then the source
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")   # also evicts from the cache

# Shutdown
await source.stop()
```

Missing price rules: SSE omits the ticker, the watchlist API returns `null`, portfolio
valuation falls back to `avg_cost` and flags it stale, and **trade execution rejects the
trade with HTTP 503** — never fill at a fabricated price.

## Environment

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty selects Massive; otherwise the simulator |
| `MASSIVE_POLL_INTERVAL` | `15.0` | Seconds between Massive polls (2–5 on a real-time plan) |

## Commands

```bash
cd backend
uv sync --extra dev
uv run pytest -q                      # 210 tests
uv run pytest --cov=app               # coverage
uv run ruff check app/ tests/         # lint
uv run market_data_demo.py            # live terminal dashboard, 60s
```
