"""GBM-based market simulator.

The module splits in two, and that split is what makes the maths testable:

    GBMSimulator        - the model. Synchronous, no asyncio, no cache, no I/O.
    SimulatorDataSource - the plumbing. Owns the asyncio task and the cache,
                          and knows nothing about GBM.
"""

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

    where mu is annualised drift, sigma annualised volatility, dt the time step
    as a fraction of a *trading* year, and Z a standard normal draw correlated
    across tickers via a Cholesky factor.

    The -sigma^2/2 term is the Ito correction. Without it the realised drift
    systematically exceeds the configured mu.
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
        # and unknown-ticker seed prices. Injectable so tests are deterministic
        # — seeding only one of the two global streams would not be enough.
        self._rng = rng if rng is not None else np.random.default_rng()
        self._shock_rng = shock_rng if shock_rng is not None else random.Random()

        # Ordered: the list order defines the correlation matrix index mapping.
        self._tickers: list[str] = []
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

            # Random event: a 2-5% jump, for visual drama. At p=0.001 with 10
            # tickers at 2 ticks/sec that is roughly one event every 50 seconds.
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
            # Rounding the state would accumulate error and could freeze a
            # low-priced ticker whose per-tick move is under half a cent.
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
        """Refactor the correlation matrix. O(n^2), only on add/remove."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        # Safe by construction: the minimum eigenvalue of this block structure
        # is 0.40. Re-verify if the correlation constants ever change — a
        # LinAlgError here would surface as a failure to add a watchlist ticker.
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based correlation between two tickers.

        Tech/tech 0.6, finance/finance 0.5, anything involving TSLA 0.3,
        everything else (cross-sector, unknown symbols) 0.3.
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is nominally tech but deliberately decoupled — it gives the
        # watchlist one ticker that visibly does its own thing.
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task that steps the simulation every
    `update_interval` seconds and writes the results to the PriceCache.

    Note that `update_interval` and GBMSimulator.DEFAULT_DT are coupled:
    DEFAULT_DT encodes the 500ms assumption, so changing the interval without
    changing dt scales every ticker's effective volatility with it.
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
