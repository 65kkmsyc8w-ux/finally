"""Factory for creating market data sources.

This is the only module in the codebase that reads MASSIVE_API_KEY. Adding a
third provider means adding a branch here and implementing MarketDataSource;
nothing else changes.
"""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 15.0


class MassiveWithSimulatorFallback(MarketDataSource):
    """Massive, degrading to the simulator if the key or plan is unusable.

    A free-tier key produces a 403 on the first poll — snapshot endpoints need
    Starter or above — and the honest response is to fall back rather than show
    a dead watchlist forever. Composition, not inheritance: MassiveDataSource
    stays a plain client and this wrapper owns the switch.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._cache = price_cache
        self._tickers: list[str] = []
        self._active: MarketDataSource = MassiveDataSource(
            api_key=api_key,
            price_cache=price_cache,
            poll_interval=poll_interval,
            on_fatal=self._switch_to_simulator,
        )

    @property
    def active(self) -> MarketDataSource:
        """The source currently producing prices."""
        return self._active

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
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
        """Replace the failed Massive poller with the simulator.

        Loud on purpose: "why am I seeing fake prices?" is the most likely
        confusion in this project, and the answer belongs in the log.
        """
        logger.error(
            "Falling back to the simulator — real market data is unavailable (%s). "
            "Massive snapshot endpoints require a Starter plan or above.",
            reason,
        )
        # Safe to stop the poller from here: this runs in its own task, and the
        # poll loop has already exited by the time on_fatal is scheduled.
        await self._active.stop()
        self._active = SimulatorDataSource(price_cache=self._cache)
        await self._active.start(self._tickers)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select a market data source from the environment.

        MASSIVE_API_KEY set and non-empty  -> Massive REST poller (real data)
        otherwise                          -> GBM simulator

    Returns an *unstarted* source: construction has no side effects, and the
    caller owns `await source.start(tickers)`. That keeps the factory trivially
    testable and puts startup ordering under the app's control.
    """
    # .strip() first: MASSIVE_API_KEY= and MASSIVE_API_KEY="   " in a .env file
    # both mean "not set". Without it, a stray space routes to a live client
    # that then fails auth.
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (real data), %.1fs poll", interval)
        return MassiveWithSimulatorFallback(
            api_key=api_key,
            price_cache=price_cache,
            poll_interval=interval,
        )

    logger.info("Market data source: GBM Simulator (set MASSIVE_API_KEY for real data)")
    return SimulatorDataSource(price_cache=price_cache)


def _poll_interval_from_env() -> float:
    """Read MASSIVE_POLL_INTERVAL, falling back to the default if unparseable."""
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_POLL_INTERVAL
    try:
        interval = float(raw)
    except ValueError:
        logger.warning("Invalid MASSIVE_POLL_INTERVAL=%r, using default", raw)
        return DEFAULT_POLL_INTERVAL
    if interval <= 0:
        logger.warning("MASSIVE_POLL_INTERVAL must be positive, using default")
        return DEFAULT_POLL_INTERVAL
    return interval
