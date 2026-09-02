# Market Simulator

Approach and code structure for the built-in price simulator — the default market data
source, used whenever `MASSIVE_API_KEY` is unset.

Companion documents: [MARKET_INTERFACE.md](MARKET_INTERFACE.md) for the interface it
implements, [MASSIVE_API.md](MASSIVE_API.md) for the real-data alternative.

---

## 1. What it has to achieve

The simulator is not a research tool. It exists so that a student who has cloned the
repo and run one Docker command sees a trading terminal that *looks alive*, at 2 a.m. on
a Sunday, with no API key.

That gives it four requirements, in priority order:

1. **Always running.** No market hours, no network, no key, no rate limit.
2. **Visually convincing at a 500 ms cadence.** Small, noisy, mostly-sub-cent moves that
   accumulate into believable intraday drift. A stock that jumps a dollar every tick
   looks broken; one that never moves looks broken too.
3. **Correlated.** When the tech block moves together the watchlist reads like a market
   rather than ten independent noise generators. This is the single detail that makes
   the screen look real.
4. **Cheap.** It runs on the event loop alongside everything else, forever.

Explicit non-goals: no order book, no bid/ask, no volume modelling, no mean reversion,
no calibration to real returns. Market orders fill instantly at the cached price, so
nothing downstream needs microstructure.

---

## 2. The model: correlated geometric Brownian motion

GBM is the standard model for equity prices and has the properties that matter here: it
cannot go negative, its moves scale with price level (a $2 move in NVDA at $800 and a
$0.50 move in JPM at $195 are the same *relative* move), and it composes over arbitrary
time steps.

Discretised, per tick:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

| Symbol | Meaning |
|---|---|
| `S(t)` | Current price |
| `μ` | Annualised drift (expected return) |
| `σ` | Annualised volatility |
| `dt` | Time step as a fraction of a trading year |
| `Z` | Standard normal draw, **correlated across tickers** |

The `−σ²/2` term is the Itô correction. Without it, `E[S(t)] = S(0)·e^(μ+σ²/2)·t` and the
simulated drift systematically exceeds the μ you configured — an easy thing to omit and
a hard thing to notice.

### Choosing `dt`

A trading year is not a calendar year. Prices only move while the market is open:

```
252 trading days × 6.5 hours × 3600 seconds = 5,896,800 seconds
dt = 0.5 / 5,896,800 ≈ 8.479e-8
```

Using 500 ms of *wall-clock* time as 500 ms of *trading* time means an hour of watching
the demo produces roughly an hour of realistic market movement. Using calendar seconds
instead (31.5M/year) would make everything look flat and dead.

The resulting per-tick moves, computed from the configured parameters:

| Ticker | σ | Price | 1-tick σ | 1-minute σ | 1-hour σ |
|---|---|---|---|---|---|
| AAPL | 0.22 | $190 | $0.012 | $0.13 | $1.03 (0.54%) |
| TSLA | 0.50 | $250 | $0.036 | $0.40 | $3.09 (1.24%) |
| NVDA | 0.40 | $800 | $0.093 | $1.02 | $7.91 (0.99%) |

Roughly a cent per tick on a $190 stock, about 1% of movement per hour. Enough that the
flash animation fires constantly; small enough that nothing looks unhinged.

### Correlation via Cholesky

Independent draws per ticker produce a watchlist where AAPL rises while MSFT falls while
GOOGL is flat — visibly wrong to anyone who has watched a real market.

The fix is standard. Build a correlation matrix `C`, factor it as `C = L·Lᵀ` (Cholesky),
and transform independent draws:

```python
z_independent = np.random.standard_normal(n)     # z ~ N(0, I)
z_correlated  = L @ z_independent                # Cov = L·Lᵀ = C
```

The correlation structure is sector-based:

| Relationship | ρ |
|---|---|
| Tech ↔ tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Finance ↔ finance (JPM, V) | 0.5 |
| TSLA ↔ anything | 0.3 |
| Cross-sector, or any unknown ticker | 0.3 |

TSLA is carved out of the tech block deliberately — it decouples from the sector often
enough in reality that the special case is more realistic than the general rule, and it
gives the watchlist one ticker that visibly does its own thing.

**On positive-definiteness.** `np.linalg.cholesky` raises `LinAlgError` if the matrix is
not positive definite, and this runs inside `add_ticker()` — a crash there would be a
user-facing failure when someone adds a symbol. This block structure is safe: its
minimum eigenvalue is **0.40** (= 1 − 0.6, set by the tech block) and stays at 0.40
regardless of how many tickers are added, verified up to n=50 with arbitrary unknown
symbols. There is comfortable margin, but any future change to these coefficients should
re-check the minimum eigenvalue rather than assume it.

`O(n²)` rebuild on every add/remove is fine for a watchlist of tens of tickers.

---

## 3. Random shock events

Every tick, every ticker gets a `p = 0.001` chance of a one-off multiplicative jump of
2–5% in a random direction, on top of the GBM step.

The intent is drama: something visible should happen while the user is watching. At 10
tickers and 2 ticks/second that is **1.2 events per minute across the watchlist**, or one
event roughly every 50 seconds — about right for a demo.

**Two consequences worth being explicit about**, because they are not obvious from the
parameters:

1. **Shocks dominate the price process.** They contribute ~9.7% hourly standard
   deviation per ticker, against ~1% from the GBM diffusion itself. The carefully chosen
   per-ticker σ values are, in practice, a second-order effect on how much a price moves.
2. **Shocks impose a downward drift.** A symmetric multiplicative jump has negative
   expected log return, since `(1+x)(1−x) < 1`. Here `E[log(1+shock)] ≈ −0.000624` per
   event × 7.2 events/hour/ticker = **−0.45% per hour**, against a configured μ = 0.05
   contributing +0.003% per hour. Over a long-running container, prices trend down.

Neither breaks the demo — a session lasts minutes, and the portfolio is fake money — but
they should be a deliberate choice rather than a surprise. If prices drifting toward
zero in a long-lived deployment becomes a problem, the options in order of preference
are: lower `event_probability` to ~0.0002 (one event every ~4 minutes, restoring GBM as
the dominant term); make shocks log-symmetric (`exp(±x)` instead of `1±x`) to remove the
drag; or add a slow pull back toward the seed price.

---

## 4. Code structure

```
backend/app/market/
├── seed_prices.py   # Starting prices, per-ticker μ/σ, correlation groups — pure data
├── simulator.py     # GBMSimulator (the math) + SimulatorDataSource (the interface)
├── models.py        # PriceUpdate
├── cache.py         # PriceCache
└── interface.py     # MarketDataSource ABC
```

The important split is inside `simulator.py`: **`GBMSimulator` contains the model and
knows nothing about asyncio, caches, or FastAPI. `SimulatorDataSource` contains the
plumbing and knows nothing about GBM.**

That boundary is what makes the math testable. `GBMSimulator.step()` is a synchronous,
pure-ish function returning a dict — statistical tests over 100k steps run in
milliseconds with no event loop, no sleeping, and no mocking.

### `seed_prices.py` — configuration as data

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00,  "JPM": 195.00,  "V": 280.00,    "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # high vol
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # high vol, strong drift
    "JPM":  {"sigma": 0.18, "mu": 0.04},   # low vol (bank)
    ...
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}   # unknown tickers

CORRELATION_GROUPS = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR, INTRA_FINANCE_CORR, CROSS_GROUP_CORR, TSLA_CORR = 0.6, 0.5, 0.3, 0.3
```

Kept as a separate module with no imports so that tuning the simulation never means
touching logic, and so tests can import the constants to assert against them.

Seed prices are plausible-as-of-authoring, not current. They do not need to be accurate —
they need to be recognisable. An unknown ticker seeds at a uniform random price in
`[50, 300]`, which is enough for a symbol someone types into the watchlist to behave.

### `GBMSimulator` — the model

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600      # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR      # ~8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None: ...

    def step(self) -> dict[str, float]:      # advance one tick → {ticker: price}
    def add_ticker(self, ticker: str) -> None
    def remove_ticker(self, ticker: str) -> None
    def get_price(self, ticker: str) -> float | None
    def get_tickers(self) -> list[str]
```

Internal state: `_tickers` (ordered — the order defines the correlation matrix's index
mapping), `_prices`, `_params`, and `_cholesky`.

`step()` is the hot path — it runs 2× per second forever:

```python
def step(self) -> dict[str, float]:
    n = len(self._tickers)
    if n == 0:
        return {}

    z = np.random.standard_normal(n)
    if self._cholesky is not None:
        z = self._cholesky @ z

    result = {}
    for i, ticker in enumerate(self._tickers):
        mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]

        drift     = (mu - 0.5 * sigma**2) * self._dt
        diffusion = sigma * math.sqrt(self._dt) * z[i]
        self._prices[ticker] *= math.exp(drift + diffusion)

        if random.random() < self._event_prob:
            magnitude = random.uniform(0.02, 0.05)
            self._prices[ticker] *= 1 + magnitude * random.choice([-1, 1])

        result[ticker] = round(self._prices[ticker], 2)
    return result
```

Three deliberate choices:

- **`_prices` holds full precision; only the returned dict is rounded.** Rounding the
  state itself would accumulate error and, worse, could freeze a low-priced ticker whose
  per-tick move is below half a cent.
- **One vectorised `standard_normal(n)` call per tick**, not one per ticker. The
  per-ticker loop is scalar `math.exp` rather than a numpy vector op — at n≈10 the
  numpy call overhead exceeds the gain, and the scalar path is clearer.
- **`add_ticker` rebuilds Cholesky; `_add_ticker_internal` does not.** Construction adds
  all tickers then factors once, rather than factoring n times.

### `SimulatorDataSource` — the plumbing

Implements `MarketDataSource`. It owns the asyncio task and the cache reference; all
model behaviour delegates to `GBMSimulator`.

```python
async def start(self, tickers: list[str]) -> None:
    self._sim = GBMSimulator(tickers, event_probability=self._event_prob)
    # Seed the cache synchronously so SSE has data before the first tick.
    for ticker in tickers:
        price = self._sim.get_price(ticker)
        if price is not None:
            self._cache.update(ticker=ticker, price=price)
    self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

async def _run_loop(self) -> None:
    while True:
        try:
            if self._sim:
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker=ticker, price=price)
        except Exception:
            logger.exception("Simulator step failed")   # never kill the loop
        await asyncio.sleep(self._interval)
```

The pre-seeding in `start()` matters: without it a browser connecting in the first
500 ms sees an empty watchlist. `add_ticker()` seeds the same way, so a newly added
ticker has a price immediately rather than on the next tick.

The bare `except` around the step is intentional. An uncaught exception inside an
`asyncio.Task` kills the task *silently* — prices would simply stop, with no error
anywhere the user can see. Catching, logging, and continuing means a transient fault
costs one tick.

`sleep(self._interval)` rather than a deadline-corrected schedule means the cadence
drifts slightly under load. For a visual demo this is invisible and not worth the
complexity; note only that tick count is therefore not a reliable clock.

---

## 5. Testing

Because `GBMSimulator` is synchronous and dependency-free, the model can be tested
statistically rather than by example.

| Property | How |
|---|---|
| Prices stay positive | 100k steps, assert `min > 0` — guaranteed by construction, but the assertion catches a regression to additive updates |
| Realised vol matches σ | Run with `event_probability=0`, take log returns, compare sample sd against `σ·√dt` within tolerance |
| Correlation is applied | Run two tech tickers with `event_probability=0`, correlate log returns, assert ≈ 0.6 (wide tolerance — this is a sampling estimate) |
| Drift has no `σ²/2` bias | Long run with σ=0, assert the realised log return tracks `μ·dt·steps` |
| Cholesky stays valid | Add and remove tickers in sequence, assert `step()` never raises |
| Unknown tickers work | `add_ticker("ZZZZ")`, assert a price appears in `[50, 300]` and defaults are applied |
| Shock magnitude bounds | Force `event_probability=1.0`, assert every move is within GBM noise + [2%, 5%] |

**Seeding for determinism requires seeding both RNGs.** The diffusion uses
`np.random.standard_normal` and the shock uses the stdlib `random` module — two
independent global streams. A test that seeds only one will still be non-deterministic.
Tests should call both `np.random.seed(...)` and `random.seed(...)`, or the simulator
should be refactored to hold injected `np.random.Generator` and `random.Random`
instances — the cleaner fix, and worth doing if determinism is ever needed beyond tests.

`SimulatorDataSource` is tested separately for lifecycle behaviour: `start()` populates
the cache before returning, `stop()` is idempotent and cancels the task,
`remove_ticker()` evicts from the cache, and the loop survives an injected exception.
These use a short `update_interval` and a real `PriceCache`.

---

## 6. Tuning

| Parameter | Where | Effect of raising it |
|---|---|---|
| `update_interval` | `SimulatorDataSource` | Slower ticks. Also raise `dt` proportionally or volatility drops with it |
| `dt` | `GBMSimulator` | Larger moves per tick. `DEFAULT_DT` assumes 500 ms ticks — the two must stay in step |
| `sigma` | `seed_prices.TICKER_PARAMS` | More movement. Dominated by shocks at current settings — see §3 |
| `mu` | `seed_prices.TICKER_PARAMS` | Long-run trend. Negligible at demo timescales |
| `event_probability` | `SimulatorDataSource` | More shocks. The single most impactful knob on how the demo *feels* |
| Correlation constants | `seed_prices` | How much the watchlist moves as a bloc. Re-check the minimum eigenvalue if changed |

The relationship between `update_interval` and `dt` is the one that catches people:
`dt` is defined as `0.5 / TRADING_SECONDS_PER_YEAR`, hard-coding the 500 ms assumption.
Change the interval to 1 second without changing `dt` and every stock's effective
volatility halves.

---

## 7. Possible extensions

- **Intraday volume profile** — real volatility is U-shaped across the session, high at
  open and close. Scaling σ by time-of-day would add realism at low cost.
- **Retained tick history** — the simulator already generates every tick but discards
  all but the latest. Keeping a bounded deque per ticker would let the detail chart show
  history immediately on page load instead of accumulating from the SSE stream.
- **Injected RNG** — as noted in §5, would make the whole simulation reproducible from a
  seed, which is valuable for E2E tests that want a known price path.
- **Scripted scenarios** — a flash crash or a sector rotation on demand, for
  demonstrating how the AI assistant reacts to a moving market.
