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

    A threading.Lock (not asyncio.Lock) because the Massive poller writes from
    a worker thread via asyncio.to_thread, while the simulator writes from the
    event loop.
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
        GIL, and a stale read costs at most one SSE cycle of latency.
        """
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
