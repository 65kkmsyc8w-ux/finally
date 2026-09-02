"""Lifecycle tests for SimulatorDataSource.

The maths is covered in test_simulator.py; this file is about the asyncio
plumbing and the cache contract.
"""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

from app.market import PriceCache
from app.market.seed_prices import SEED_PRICES
from app.market.simulator import SimulatorDataSource

FAST = 0.01  # Tick interval for tests


@pytest.fixture
async def source(cache: PriceCache):
    src = SimulatorDataSource(price_cache=cache, update_interval=FAST)
    yield src
    await src.stop()


async def test_start_seeds_the_cache_before_returning(source, cache: PriceCache):
    """A browser connecting in the first 500ms must not see an empty watchlist."""
    await source.start(["AAPL", "MSFT"])
    assert cache.get_price("AAPL") == SEED_PRICES["AAPL"]
    assert cache.get_price("MSFT") == SEED_PRICES["MSFT"]


async def test_start_normalises_and_deduplicates(source):
    await source.start(["aapl", " AAPL ", "msft"])
    assert source.get_tickers() == ["AAPL", "MSFT"]


async def test_start_with_no_tickers_is_fine(source, cache: PriceCache):
    """An empty watchlist is a normal state, not an error."""
    await source.start([])
    await asyncio.sleep(FAST * 3)
    assert source.get_tickers() == []
    assert len(cache) == 0


async def test_prices_update_over_time(source, cache: PriceCache):
    await source.start(["AAPL"])
    version = cache.version
    await asyncio.sleep(FAST * 5)
    assert cache.version > version
    assert cache.get("AAPL").timestamp > 0


async def test_get_tickers_is_empty_before_start(cache: PriceCache):
    assert SimulatorDataSource(price_cache=cache).get_tickers() == []


async def test_add_ticker_seeds_a_price_immediately(source, cache: PriceCache):
    await source.start(["AAPL"])
    await source.add_ticker("tsla")
    assert cache.get_price("TSLA") == SEED_PRICES["TSLA"]  # not "on the next tick"
    assert "TSLA" in source.get_tickers()


async def test_add_ticker_before_start_is_a_noop(source, cache: PriceCache):
    await source.add_ticker("AAPL")
    assert cache.get("AAPL") is None


async def test_remove_ticker_evicts_from_the_cache(source, cache: PriceCache):
    """Otherwise the removed ticker keeps appearing in the SSE payload."""
    await source.start(["AAPL", "MSFT"])
    await source.remove_ticker("aapl")
    assert cache.get("AAPL") is None
    assert "AAPL" not in source.get_tickers()
    await asyncio.sleep(FAST * 3)
    assert cache.get("AAPL") is None  # and the loop does not re-add it


async def test_remove_unknown_ticker_is_a_noop(source):
    await source.start(["AAPL"])
    await source.remove_ticker("NOPE")
    assert source.get_tickers() == ["AAPL"]


async def test_stop_cancels_the_background_task(source, cache: PriceCache):
    await source.start(["AAPL"])
    await source.stop()
    version = cache.version
    await asyncio.sleep(FAST * 5)
    assert cache.version == version  # nothing written after stop()


async def test_stop_is_idempotent(source):
    await source.start(["AAPL"])
    await source.stop()
    await source.stop()  # must not raise


async def test_stop_without_start_is_safe(cache: PriceCache):
    await SimulatorDataSource(price_cache=cache).stop()


async def test_loop_survives_a_failing_step(source, cache: PriceCache, caplog):
    """An escaping exception would kill the task silently and freeze prices."""
    await source.start(["AAPL"])
    source._sim.step = Mock(side_effect=RuntimeError("boom"))
    await asyncio.sleep(FAST * 4)
    assert not source._task.done()
    assert "Simulator step failed" in caplog.text

    # And it recovers once the fault clears.
    source._sim.step = Mock(return_value={"AAPL": 191.0})
    await asyncio.sleep(FAST * 3)
    assert cache.get_price("AAPL") == 191.0


async def test_event_probability_is_passed_through(cache: PriceCache):
    src = SimulatorDataSource(price_cache=cache, update_interval=FAST, event_probability=0.5)
    await src.start(["AAPL"])
    assert src._sim._event_prob == 0.5
    await src.stop()
