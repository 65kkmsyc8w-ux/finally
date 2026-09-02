"""Massive (formerly Polygon.io) API client for real market data.

Polls the v3 unified snapshot endpoint for every watched ticker in a single
request per cycle, and writes the results into the shared PriceCache.

Field names here were verified against massive==2.2.0. Getting one wrong is
the most likely defect in this module, which is why field access below is
explicit rather than wrapped in `except AttributeError` — a wrong name must
fail loudly in tests, not degrade into a silent "no data" condition.
"""

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

# The v3 snapshot endpoint defaults to limit=10 and caps at 250. Passing it
# explicitly is mandatory: without it a watchlist of 12 silently receives the
# first ten prices and the rest never appear.
SNAPSHOT_LIMIT = 250

# Substrings identifying a permanently unusable key or plan. BadResponse
# carries only the decoded response body — there is no status code attribute —
# so terminal errors are recognised from the body text. See is_terminal_error().
TERMINAL_MARKERS = (
    "not_authorized",
    "not entitled",
    "unknown api key",
    "invalid api key",
    "upgrade your plan",
)


def extract_price(snap: UniversalSnapshot) -> tuple[float, float | None] | None:
    """Pull (price, timestamp_seconds) out of a v3 snapshot.

    Ordered by freshness: last trade, then the session price (populated
    outside market hours), then the previous close. Returns None when the
    snapshot carries no usable price at all — a fabricated price would corrupt
    the portfolio permanently.
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

    # 2. Session price — populated when the market is closed, no timestamp.
    if session is not None and session.price is not None:
        return float(session.price), None

    # 3. Previous close — the honest floor: a stale real price beats none.
    if session is not None and session.previous_close is not None:
        return float(session.previous_close), None

    return None


def is_terminal_error(exc: Exception) -> bool:
    """Classify an API failure as terminal (bad key / unlicensed plan).

    401 bodies say "Unknown API Key"; 403 bodies say "NOT_AUTHORIZED" or
    "not entitled ... upgrade your plan" — that is what a free Basic plan
    returns for snapshot endpoints. Everything else (429, 5xx, timeouts) is
    transient and worth retrying on the next cycle.
    """
    if isinstance(exc, AuthError):
        return True
    if isinstance(exc, BadResponse):
        body = str(exc).lower()
        return any(marker in body for marker in TERMINAL_MARKERS)
    return False


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive REST API.

    One request per cycle covers the whole watchlist, which is what keeps this
    inside a 5-calls-per-minute free-tier budget.

    Poll interval guidance:
        Starter / Developer (15-min delayed):  5-15s
        Advanced / Business (real-time):       2-5s
        Basic (free):                          snapshots return 403 — the
                                               source degrades via on_fatal.
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

    @property
    def degraded(self) -> bool:
        """True once a terminal error has stopped the poller for good."""
        return self._degraded

    async def start(self, tickers: list[str]) -> None:
        # RESTClient raises AuthError at construction on an empty key, so a
        # missing key fails immediately rather than on the first request.
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))

        # Poll once before returning so the cache is populated for the first
        # SSE connection instead of a whole interval later.
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
                # retried internally; the next cycle will try again.
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

        The massive RESTClient is urllib3-based and blocking; calling it
        directly on the event loop would stall every SSE stream in the process.
        """
        assert self._client is not None
        return list(
            self._client.list_universal_snapshots(
                type="stocks",
                ticker_any_of=tickers,
                limit=SNAPSHOT_LIMIT,
            )
        )

    def _enter_degraded(self, reason: str) -> None:
        """Stop polling permanently and notify the owner.

        Terminal means the key is wrong or the plan does not license snapshot
        endpoints. Retrying that every 15 seconds forever would just fill the
        log while the watchlist stays empty.

        on_fatal is scheduled as its own task rather than awaited: it runs from
        inside the poll task, and a handler that calls stop() on this source
        would otherwise cancel and await the very task it is running on.
        """
        if self._degraded:
            return
        self._degraded = True
        logger.error("Massive API unusable, stopping poller: %s", reason)
        if self._on_fatal is not None:
            asyncio.create_task(self._on_fatal(reason), name="massive-fatal")
