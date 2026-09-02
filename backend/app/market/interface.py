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
        watchlist. Called exactly once; calling start() twice is undefined
        behaviour.
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
        """Add a ticker to the active set. No-op if already present.

        Eventually consistent: the simulator can seed a price instantly, the
        Massive poller cannot until its next cycle.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set *and evict it from the cache*.

        No-op if the ticker is not tracked.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers. Synchronous — it reads in-memory state."""
