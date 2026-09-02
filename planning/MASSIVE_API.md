# Massive API Reference (formerly Polygon.io)

Reference for the Massive REST API as consumed by FinAlly's optional real-market-data
path. Everything below was verified against the **`massive` Python client v2.2.0** (the
version pinned in `backend/uv.lock`) by inspecting the installed package, plus the
official docs at <https://massive.com/docs>.

> **Read this first if you are touching `backend/app/market/massive_client.py`.**
> The field names in this document are the real ones. See
> [Known defect in the current client](#known-defect-in-the-current-client).

---

## 1. Orientation

Polygon.io rebranded to **Massive** on 30 October 2025. It is the same platform,
same accounts, same API keys.

| Item | Value |
|---|---|
| Docs | <https://massive.com/docs> (machine-readable index: `https://massive.com/docs/llms.txt`) |
| Base URL | `https://api.massive.com` |
| Legacy base URL | `https://api.polygon.io` — still supported during an extended transition |
| Python package | `massive` — `uv add massive` / `pip install -U massive` |
| Client version used here | `2.2.0` |
| Auth | `Authorization: Bearer <API_KEY>` header (the client sets this for you) |
| Env var | `MASSIVE_API_KEY` — read automatically by `RESTClient()` |
| Timestamps | Unix **nanoseconds** on v2 trade/quote objects; **milliseconds** on aggregate bars |

### Rate limits and plans

| Plan | Cost | API calls | Data recency |
|---|---|---|---|
| Basic | Free | **5 / minute** | End-of-day |
| Starter | Paid | Unlimited | 15-minute delayed |
| Developer | Paid | Unlimited | 15-minute delayed |
| Advanced | Paid | Unlimited | Real-time |
| Business | Paid | Unlimited | Real-time |

Paid plans lift the request meter entirely — there is no per-request billing and no
overage. Massive's guidance is simply to stay reasonable (under ~100 req/s).

**Consequence for FinAlly:** the free Basic tier at 5 calls/minute is the binding
constraint. This is why the design polls **one snapshot endpoint for every watched
ticker in a single request** rather than looping per ticker. At a 15-second interval
that is 4 calls/minute — inside the free budget with headroom for a retry.

Note also that snapshot endpoints require **Starter or above**. On the free Basic plan
the snapshot call returns `403`, and only end-of-day endpoints such as
`/v2/aggs/ticker/{ticker}/prev` are available. FinAlly must degrade gracefully in that
case — see [Error handling](#7-error-handling).

---

## 2. Client setup

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment automatically.
client = RESTClient()

# Or pass it explicitly (what FinAlly's factory does).
client = RESTClient(api_key="your_key_here")
```

Full constructor, with the defaults that matter:

```python
RESTClient(
    api_key: str | None = os.getenv("MASSIVE_API_KEY"),
    connect_timeout: float = 10.0,
    read_timeout: float = 10.0,
    num_pools: int = 10,
    retries: int = 3,                       # auto-retries 413/429/499/500/502/503/504
    base: str = "https://api.massive.com",  # override to hit api.polygon.io
    pagination: bool = True,
)
```

Two behaviours worth knowing:

- **`api_key=None` raises `AuthError` at construction time**, not on first request. A
  missing key fails loudly and immediately.
- **Retries are built in.** `retries=3` with a `backoff_factor` of 0.1 covers `429`
  automatically. Do not add your own retry loop on top of it — you will multiply the
  request count against a 5/minute budget.

**The client is synchronous.** It uses `urllib3`, not `httpx`/`aiohttp`. Every call from
FinAlly's async event loop must be wrapped:

```python
snapshots = await asyncio.to_thread(client.get_snapshot_all, "stocks", tickers)
```

---

## 3. The endpoint FinAlly polls

### Option A — v2 Full Market Snapshot (what the code uses today)

One request returns the current state of every requested ticker.

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

| Parameter | Type | Notes |
|---|---|---|
| `tickers` | string | Case-sensitive, comma-separated. Omit to get **every** US ticker. |
| `include_otc` | bool | Default `false`. |

```bash
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,TSLA" \
  -H "Authorization: Bearer $MASSIVE_API_KEY"
```

Python:

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.todays_change_percent)
```

Raw JSON shape (short keys — the client maps these for you):

```json
{
  "status": "OK",
  "count": 3,
  "tickers": [
    {
      "ticker": "AAPL",
      "day":     {"o": 129.6, "h": 130.2, "l": 125.1, "c": 125.1, "v": 111237700, "vw": 127.35},
      "prevDay": {"o": 128.0, "h": 130.0, "l": 127.5, "c": 129.6, "v": 98000000,  "vw": 128.90},
      "min":     {"t": 1675190340000, "o": 125.0, "h": 125.2, "l": 124.9, "c": 125.07, "v": 12000},
      "lastTrade": {"t": 1675190399000000000, "p": 125.07, "s": 100, "x": 4, "i": "12345", "c": [14]},
      "lastQuote": {"t": 1675190399500000000, "p": 125.06, "s": 5, "P": 125.08, "S": 10},
      "todaysChange": -4.54,
      "todaysChangePerc": -3.50,
      "updated": 1675190399999999999
    }
  ]
}
```

#### Object model — the exact attribute names

This is the part that is easy to get wrong. `TickerSnapshot` **does not** have a
`day.previous_close` or a `day.change_percent`, and `last_trade` **does not** have a
`.timestamp`.

`TickerSnapshot`:

| Attribute | JSON key | Meaning |
|---|---|---|
| `ticker` | `ticker` | Symbol |
| `day` | `day` | `Agg` — today's bar so far |
| `prev_day` | `prevDay` | `Agg` — previous session's bar |
| `min` | `min` | `MinuteSnapshot` — most recent minute bar |
| `last_trade` | `lastTrade` | `LastTrade` |
| `last_quote` | `lastQuote` | `LastQuote` |
| `todays_change` | `todaysChange` | Absolute change vs. previous close |
| `todays_change_percent` | `todaysChangePerc` | Percentage change vs. previous close |
| `updated` | `updated` | Nanosecond timestamp |
| `fair_market_value` | `fmv` | Business plans only |

`Agg` (used for both `day` and `prev_day`):

| Attribute | JSON key |
|---|---|
| `open` | `o` |
| `high` | `h` |
| `low` | `l` |
| `close` | `c` |
| `volume` | `v` |
| `vwap` | `vw` |
| `timestamp` | `t` (Unix **milliseconds**) |
| `transactions` | `n` |
| `otc` | `otc` |

`LastTrade` — note there is no plain `timestamp`:

| Attribute | JSON key | Meaning |
|---|---|---|
| `price` | `p` | Trade price |
| `size` | `s` | Trade size |
| `exchange` | `x` | Exchange ID |
| `sip_timestamp` | `t` | Unix **nanoseconds** ← use this one |
| `participant_timestamp` | `y` | Unix nanoseconds |
| `trf_timestamp` | `f` | Unix nanoseconds |
| `conditions` | `c` | Condition codes |
| `id` | `i` | Trade ID |
| `sequence_number` | `q` | |
| `tape` | `z` | |

`LastQuote`: `bid_price`, `bid_size`, `bid_exchange`, `ask_price`, `ask_size`,
`ask_exchange`, `sip_timestamp`, `participant_timestamp`, `conditions`, `tape`.

#### Deriving what FinAlly needs

```python
price          = snap.last_trade.price          # current price
previous_close = snap.prev_day.close            # NOT snap.day.previous_close
day_change_pct = snap.todays_change_percent     # NOT snap.day.change_percent
ts_seconds     = snap.last_trade.sip_timestamp / 1e9   # nanoseconds → seconds
```

### Option B — v3 Unified Snapshot (recommended for a rewrite)

Newer, cleaner, and a better shape for FinAlly because a single `session` object carries
price, previous close, and change percentage together.

```
GET /v3/snapshot?ticker.any_of=AAPL,NVDA&limit=250
```

| Parameter | Notes |
|---|---|
| `ticker.any_of` | Comma-separated, **max 250 tickers** |
| `type` | Asset class filter (`stocks`, `options`, `fx`, `crypto`, `indices`) |
| `limit` | Default 10, **max 250** — must be raised explicitly |
| `order`, `sort` | Result ordering |

```python
snapshots = list(client.list_universal_snapshots(
    type="stocks",
    ticker_any_of=["AAPL", "GOOGL", "MSFT"],
    limit=250,
))

for s in snapshots:
    print(s.ticker, s.session.price, s.session.previous_close, s.session.change_percent)
```

`UniversalSnapshot` fields: `ticker`, `type`, `session`, `last_quote`, `last_trade`,
`last_minute`, `market_status`, `name`, `fair_market_value`, `error`, `message`,
`last_updated`, `timeframe`.

`UniversalSnapshotSession` — the useful one:

`price`, `change`, `change_percent`, `open`, `close`, `high`, `low`,
`previous_close`, `volume`, `vwap`, `last_updated`, plus early/regular/late
trading change breakdowns.

Two real advantages over v2:

1. `session.price` is populated even when there is no recent trade, so you get a usable
   price outside market hours without falling back to `prev_day`.
2. Per-ticker `error` / `message` fields let you detect a single bad symbol without the
   whole request failing.

The `limit` default of 10 is the trap here — FinAlly must pass `limit=250` or it will
silently receive only the first ten tickers.

---

## 4. Supporting endpoints

### Previous close — the free-tier fallback

Available on **every** plan including free Basic. FinAlly uses this to seed prices when
the snapshot endpoint is not licensed.

```
GET /v2/aggs/ticker/{ticker}/prev?adjusted=true
```

```python
prev = client.get_previous_close_agg(ticker="AAPL")
# PreviousCloseAgg: ticker, open, high, low, close, volume, vwap, timestamp, transactions
```

One call per ticker, so ten tickers costs ten calls — over budget on the free tier in a
single minute. Fetch these once at startup, spread out, and cache.

### Single ticker snapshot

```
GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
```

```python
snap = client.get_snapshot_ticker(market_type="stocks", ticker="AAPL")
```

Returns the same `TickerSnapshot` object. Useful for a ticker detail view; wasteful for
polling a watchlist.

### Aggregate bars — for the detail chart

```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

```python
bars = list(client.list_aggs(
    ticker="AAPL",
    multiplier=5,
    timespan="minute",
    from_="2026-09-01",
    to="2026-09-02",
    limit=50000,
))
# Each bar is an Agg: .open .high .low .close .volume .vwap .timestamp (ms)
```

`timespan` accepts `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`.
`list_aggs` auto-paginates; `get_aggs` returns a single page.

This is the endpoint to reach for if the main chart ever needs real history instead of
SSE-accumulated points.

### Last trade / last quote

```python
trade = client.get_last_trade(ticker="AAPL")   # .price .size .sip_timestamp
quote = client.get_last_quote(ticker="AAPL")   # .bid_price .ask_price .bid_size .ask_size
```

One call per ticker. Not used by the poller.

### Other methods on the client

`get_grouped_daily_aggs` (whole-market daily bars in one call — a cheap way to seed
many tickers), `get_daily_open_close_agg`, `get_snapshot_direction` (gainers/losers),
`get_snapshot_indices`, `list_snapshot_options_chain`, `get_futures_snapshot`.

---

## 5. Choosing a poll interval

| Plan | Calls/min | Safe interval | Rationale |
|---|---|---|---|
| Basic (free) | 5 | snapshot unavailable (403) | Fall back to the simulator |
| Starter / Developer | Unlimited | 5–15 s | Data is 15-min delayed; faster polling gains nothing |
| Advanced / Business | Unlimited | 2–5 s | Real-time data justifies the cadence |

FinAlly's default of **15 seconds** is the conservative choice that works everywhere the
snapshot endpoint is licensed at all.

Two things stay true regardless of the interval:

- The SSE stream to the browser still pushes at ~500 ms. Between polls the frontend
  simply re-renders the same cached price. Prices will visibly step rather than drift —
  this is expected and is the honest representation of polled data.
- Outside market hours the snapshot returns the last traded price and stops changing.
  The UI should not be built to assume continuous movement.

---

## 6. Reference poller

The shape FinAlly actually uses — one call per cycle, wrapped for the event loop, with
failures confined to a single cycle.

```python
import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

logger = logging.getLogger(__name__)


async def poll_once(client: RESTClient, tickers: list[str], cache) -> None:
    """One poll cycle: fetch all tickers in a single request, write to the cache."""
    if not tickers:
        return

    # The client is synchronous urllib3 — never call it directly on the event loop.
    snapshots = await asyncio.to_thread(
        client.get_snapshot_all,
        SnapshotMarketType.STOCKS,
        tickers,
    )

    for snap in snapshots:
        trade = snap.last_trade
        if trade is None or trade.price is None:
            # No trade today (halted, pre-IPO, bad symbol). Fall back to prev close.
            price = snap.prev_day.close if snap.prev_day else None
            timestamp = None
        else:
            price = trade.price
            # sip_timestamp is nanoseconds; PriceCache wants seconds.
            timestamp = trade.sip_timestamp / 1e9 if trade.sip_timestamp else None

        if price is None:
            logger.warning("No usable price for %s", snap.ticker)
            continue

        cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)


async def poll_loop(api_key: str, get_tickers, cache, interval: float = 15.0) -> None:
    client = RESTClient(api_key=api_key)
    while True:
        try:
            await poll_once(client, get_tickers(), cache)
        except Exception:
            # Never let one bad cycle kill the loop; the next tick retries.
            logger.exception("Massive poll failed")
        await asyncio.sleep(interval)
```

---

## 7. Error handling

| Status | Cause | Response |
|---|---|---|
| `401` | Invalid or revoked API key | Log once, stop the poller, fall back to the simulator |
| `403` | Plan does not license this endpoint (free Basic + snapshots) | Same — fall back rather than retry |
| `429` | Rate limit exceeded | Client already retries with backoff; if it surfaces, lengthen the interval |
| `5xx` | Massive-side error | Client retries 3×; the poll loop retries on the next tick |
| Network / timeout | Connectivity | Retry on the next tick |

Design rules that follow from this table:

1. **Never let an exception escape the poll loop.** A raised exception in an
   `asyncio.Task` kills the task silently and prices freeze with no error visible to
   the user.
2. **Treat `401` and `403` as terminal, everything else as transient.** Retrying a bad
   key every 15 seconds forever is pure noise.
3. **A failed poll must leave the cache untouched, not cleared.** Stale prices are far
   better than an empty watchlist.
4. **Do not catch `AttributeError` as a normal control path.** Doing so converts a
   field-name mistake into a silent "no data" condition — exactly what happened below.

---

## Known defect in the current client

`backend/app/market/massive_client.py` currently reads:

```python
price = snap.last_trade.price
timestamp = snap.last_trade.timestamp / 1000.0   # ← LastTrade has no .timestamp
```

`LastTrade` exposes `sip_timestamp`, `participant_timestamp` and `trf_timestamp`, all in
**nanoseconds**. There is no `.timestamp` attribute, so this line raises
`AttributeError` for every snapshot. The surrounding
`except (AttributeError, TypeError)` swallows it and logs a per-ticker warning, so the
poller runs forever and writes **nothing** to the cache.

The fix:

```python
price = snap.last_trade.price
timestamp = snap.last_trade.sip_timestamp / 1e9
```

Because `MASSIVE_API_KEY` is empty by default, every test run and every default launch
takes the simulator path, which is why the existing unit tests pass — they mock the
snapshot objects with `Mock`, and a `Mock` answers to `.timestamp` happily. The tests
should be changed to assert against real `TickerSnapshot` / `LastTrade` instances, or at
minimum `Mock(spec=LastTrade)`, so that a wrong field name fails the suite.

---

## Sources

- [Massive API docs](https://massive.com/docs)
- [REST API quickstart](https://massive.com/docs/rest/quickstart)
- [Stocks REST API overview](https://massive.com/docs/rest/stocks/overview)
- [Full market snapshot](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Unified snapshot](https://massive.com/docs/rest/stocks/snapshots/unified-snapshot)
- [Single ticker snapshot](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot)
- [Previous day bar](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [Massive pricing](https://massive.com/pricing)
- [Official Python client](https://github.com/polygon-io/client-python)
- Package inspection: `massive==2.2.0` from PyPI
