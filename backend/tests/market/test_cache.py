"""Tests for PriceCache."""

from __future__ import annotations

import time
from threading import Thread

from app.market import PriceCache


def test_update_returns_the_stored_update(cache: PriceCache):
    update = cache.update("AAPL", 190.0)
    assert update.ticker == "AAPL"
    assert cache.get("AAPL") == update


def test_first_update_is_flat(cache: PriceCache):
    """No spurious flash on page load."""
    update = cache.update("AAPL", 190.0)
    assert update.previous_price == 190.0
    assert update.direction == "flat"


def test_second_update_carries_the_previous_price(cache: PriceCache):
    """The cache derives previous_price so sources never have to track it."""
    cache.update("AAPL", 190.0)
    update = cache.update("AAPL", 191.0)
    assert update.previous_price == 190.0
    assert update.direction == "up"


def test_prices_are_rounded_to_two_decimals(cache: PriceCache):
    """The cache is the single place display precision is decided."""
    assert cache.update("AAPL", 190.126_999).price == 190.13
    assert cache.update("AAPL", 190.124_999).previous_price == 190.13


def test_explicit_timestamp_is_used(cache: PriceCache):
    assert cache.update("AAPL", 190.0, timestamp=1_700_000_000.0).timestamp == 1_700_000_000.0


def test_explicit_zero_timestamp_is_preserved(cache: PriceCache):
    """Regression: `timestamp or time.time()` would silently replace 0.0."""
    assert cache.update("AAPL", 190.0, timestamp=0.0).timestamp == 0.0


def test_timestamp_defaults_to_now(cache: PriceCache):
    before = time.time()
    assert before <= cache.update("AAPL", 190.0).timestamp <= time.time()


def test_get_returns_none_for_unknown_ticker(cache: PriceCache):
    assert cache.get("NOPE") is None
    assert cache.get_price("NOPE") is None


def test_get_price_returns_the_float(cache: PriceCache):
    cache.update("AAPL", 190.0)
    assert cache.get_price("AAPL") == 190.0


def test_get_all_returns_a_copy(cache: PriceCache):
    """Mutating the returned dict must not corrupt the cache."""
    cache.update("AAPL", 190.0)
    snapshot = cache.get_all()
    snapshot["AAPL"] = None
    snapshot["MSFT"] = None
    assert cache.get("AAPL") is not None
    assert "MSFT" not in cache


def test_remove_evicts(cache: PriceCache):
    cache.update("AAPL", 190.0)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in cache


def test_remove_is_a_noop_for_unknown_ticker(cache: PriceCache):
    cache.remove("NOPE")  # must not raise


def test_version_increments_on_every_update(cache: PriceCache):
    assert cache.version == 0
    cache.update("AAPL", 190.0)
    cache.update("AAPL", 191.0)
    cache.update("MSFT", 420.0)
    assert cache.version == 3


def test_version_is_read_under_the_lock(cache: PriceCache):
    """Every other accessor locks; an unlocked int read has no ordering
    guarantee against the preceding dict write on a free-threaded build."""

    class CountingLock:
        def __init__(self, inner):
            self._inner, self.acquisitions = inner, 0

        def __enter__(self):
            self.acquisitions += 1
            return self._inner.__enter__()

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

    cache._lock = CountingLock(cache._lock)
    assert cache.version == 0
    assert cache._lock.acquisitions == 1


def test_reading_version_does_not_deadlock_against_writes(cache: PriceCache):
    """The lock is not reentrant, so a nested acquisition would hang here."""
    cache.update("AAPL", 190.0)
    assert cache.version == 1
    assert len(cache) == 1
    assert cache.get_price("AAPL") == 190.0  # get_price -> get, both lock
    assert "AAPL" in cache


def test_version_stays_readable_under_concurrent_writers():
    cache = PriceCache()
    seen: list[int] = []
    stop = False

    def writer(n: int) -> None:
        for i in range(200):
            cache.update(f"T{n}", 100.0 + i)

    def reader() -> None:
        while not stop:
            seen.append(cache.version)

    writers = [Thread(target=writer, args=(n,)) for n in range(5)]
    watcher = Thread(target=reader)
    watcher.start()
    for thread in writers:
        thread.start()
    for thread in writers:
        thread.join()
    stop = True
    watcher.join()

    assert cache.version == 1000
    assert seen == sorted(seen)  # monotonic: never observed going backwards
    assert max(seen) <= 1000  # never ahead of the writes that completed


def test_version_is_unchanged_by_reads_and_removes(cache: PriceCache):
    """SSE change detection must not fire on a read."""
    cache.update("AAPL", 190.0)
    version = cache.version
    cache.get("AAPL")
    cache.get_all()
    cache.get_price("AAPL")
    assert cache.version == version


def test_len_and_contains(cache: PriceCache):
    assert len(cache) == 0
    cache.update("AAPL", 190.0)
    cache.update("MSFT", 420.0)
    assert len(cache) == 2
    assert "AAPL" in cache
    assert "NOPE" not in cache


def test_concurrent_writes_are_all_recorded():
    """The Massive poller writes from a worker thread, so this is not theoretical."""
    cache = PriceCache()
    tickers = [f"T{n}" for n in range(10)]

    def hammer(ticker: str) -> None:
        for i in range(100):
            cache.update(ticker, 100.0 + i)

    threads = [Thread(target=hammer, args=(t,)) for t in tickers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(cache) == 10
    assert cache.version == 1000  # No lost updates
    assert all(cache.get_price(t) == 199.0 for t in tickers)


def test_concurrent_readers_see_consistent_snapshots():
    """get_all() must never observe a half-written dict."""
    cache = PriceCache()
    for i in range(50):
        cache.update(f"T{i}", 100.0)
    errors: list[Exception] = []

    def reader() -> None:
        try:
            for _ in range(200):
                snapshot = cache.get_all()
                assert all(u.ticker == t for t, u in snapshot.items())
        except Exception as exc:  # pragma: no cover - only on failure
            errors.append(exc)

    def writer() -> None:
        for i in range(200):
            cache.update(f"T{i % 50}", 100.0 + i)

    threads = [Thread(target=reader), Thread(target=writer), Thread(target=reader)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
