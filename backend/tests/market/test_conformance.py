"""Contract tests run against *every* MarketDataSource implementation.

This is what stops the two sources drifting apart. Nothing here knows which
implementation it is testing — if a test needs to, it belongs in
test_simulator_source.py or test_massive.py instead.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from massive.rest.models import UniversalSnapshot

from app.market import MarketDataSource, PriceCache
from app.market.factory import MassiveWithSimulatorFallback
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource

FAST = 0.01


def _snapshot(ticker: str, price: float = 100.0) -> UniversalSnapshot:
    return UniversalSnapshot.from_dict(
        {
            "ticker": ticker,
            "last_trade": {"price": price, "sip_timestamp": 1_675_190_399_000_000_000},
        }
    )


@pytest.fixture(params=["simulator", "massive", "massive-with-fallback"])
async def source(request, cache: PriceCache):
    """Every implementation, started the same way, with no network involved."""
    if request.param == "simulator":
        src: MarketDataSource = SimulatorDataSource(price_cache=cache, update_interval=FAST)
        with patch("app.market.simulator.logger"):
            yield src
    else:
        if request.param == "massive":
            src = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=FAST)
            client = src
        else:
            src = MassiveWithSimulatorFallback(api_key="k", price_cache=cache, poll_interval=FAST)
            client = src.active
        # RESTClient construction does no I/O; patching the one synchronous
        # call means nothing reaches the network.
        with (
            patch("app.market.massive_client.RESTClient"),
            patch.object(
                client,
                "_fetch_snapshots",
                side_effect=lambda tickers: [_snapshot(t) for t in tickers],
            ),
        ):
            yield src
    await src.stop()


async def test_start_seeds_the_cache_before_returning(source, cache: PriceCache):
    """Not "after the first tick" — a browser connecting immediately must see prices."""
    await source.start(["AAPL", "MSFT"])
    assert cache.get_price("AAPL") is not None
    assert cache.get_price("MSFT") is not None


async def test_start_tracks_the_requested_tickers(source):
    await source.start(["AAPL", "MSFT"])
    assert set(source.get_tickers()) == {"AAPL", "MSFT"}


async def test_tickers_are_normalised(source):
    """Massive's matching is case-sensitive, and two cases would produce two
    independent cache entries."""
    await source.start(["aapl", " msft "])
    assert set(source.get_tickers()) == {"AAPL", "MSFT"}


async def test_start_deduplicates(source):
    await source.start(["AAPL", "aapl", "AAPL"])
    assert source.get_tickers() == ["AAPL"]


async def test_start_with_an_empty_watchlist_is_valid(source, cache: PriceCache):
    await source.start([])
    await asyncio.sleep(FAST * 2)
    assert source.get_tickers() == []
    assert len(cache) == 0


async def test_get_tickers_is_empty_before_start(source):
    assert source.get_tickers() == []


async def test_add_ticker_extends_the_active_set(source):
    await source.start(["AAPL"])
    await source.add_ticker("tsla")
    assert "TSLA" in source.get_tickers()


async def test_add_ticker_is_a_noop_when_already_present(source):
    await source.start(["AAPL"])
    await source.add_ticker("AAPL")
    assert source.get_tickers().count("AAPL") == 1


async def test_added_tickers_get_a_price_eventually(source, cache: PriceCache):
    """How eventual differs by source — the simulator seeds instantly, Massive
    on its next poll — but every source must get there."""
    await source.start(["AAPL"])
    await source.add_ticker("TSLA")
    for _ in range(50):
        if cache.get_price("TSLA") is not None:
            break
        await asyncio.sleep(FAST)
    assert cache.get_price("TSLA") is not None


async def test_remove_ticker_evicts_from_the_cache(source, cache: PriceCache):
    """Otherwise the ticker keeps appearing in the SSE payload forever."""
    await source.start(["AAPL", "MSFT"])
    await source.remove_ticker("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in source.get_tickers()


async def test_removed_tickers_do_not_come_back(source, cache: PriceCache):
    await source.start(["AAPL", "MSFT"])
    await source.remove_ticker("AAPL")
    await asyncio.sleep(FAST * 4)
    assert cache.get("AAPL") is None


async def test_remove_ticker_is_a_noop_when_absent(source):
    await source.start(["AAPL"])
    await source.remove_ticker("NOPE")
    assert source.get_tickers() == ["AAPL"]


async def test_prices_keep_arriving_while_running(source, cache: PriceCache):
    await source.start(["AAPL"])
    version = cache.version
    await asyncio.sleep(FAST * 5)
    assert cache.version > version


async def test_stop_halts_all_writes(source, cache: PriceCache):
    await source.start(["AAPL"])
    await source.stop()
    version = cache.version
    await asyncio.sleep(FAST * 5)
    assert cache.version == version


async def test_stop_is_idempotent(source):
    await source.start(["AAPL"])
    await source.stop()
    await source.stop()  # must never raise — it runs during lifespan shutdown


async def test_stop_without_start_is_safe(source):
    await source.stop()


async def test_the_source_exposes_no_price_getter(source):
    """Prices come from the cache. A source that answers get_price() invites
    callers to couple to a provider."""
    assert not hasattr(source, "get_price")
