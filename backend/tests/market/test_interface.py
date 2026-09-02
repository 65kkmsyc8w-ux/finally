"""Tests for the MarketDataSource contract surface."""

from __future__ import annotations

import pytest

from app.market import MarketDataSource, PriceCache, normalize_ticker


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("aapl", "AAPL"),
        (" msft ", "MSFT"),
        ("\tnvda\n", "NVDA"),
        ("AAPL", "AAPL"),
        ("BrK.b", "BRK.B"),
    ],
)
def test_normalize_ticker(raw, expected):
    assert normalize_ticker(raw) == expected


def test_normalize_ticker_is_idempotent():
    assert normalize_ticker(normalize_ticker(" tsla ")) == "TSLA"


def test_interface_cannot_be_instantiated():
    with pytest.raises(TypeError):
        MarketDataSource()  # type: ignore[abstract]


def test_a_partial_implementation_cannot_be_instantiated():
    class Partial(MarketDataSource):
        async def start(self, tickers): ...
        async def stop(self): ...

    with pytest.raises(TypeError):
        Partial()  # type: ignore[abstract]


def test_the_interface_exposes_no_price_getter():
    """Sources push into the cache; asking a source for a price is the
    coupling this design exists to prevent."""
    assert not hasattr(MarketDataSource, "get_price")


async def test_a_fake_source_is_four_lines(cache: PriceCache):
    """The ABC is small enough that portfolio/trade tests can fake it."""

    class FakeDataSource(MarketDataSource):
        def __init__(self, cache):
            self._cache, self._tickers = cache, []

        async def start(self, tickers):
            self._tickers = list(tickers)

        async def stop(self): ...

        async def add_ticker(self, t):
            self._tickers.append(t)

        async def remove_ticker(self, t):
            self._tickers.remove(t)
            self._cache.remove(t)

        def get_tickers(self):
            return list(self._tickers)

    source = FakeDataSource(cache)
    await source.start(["AAPL"])
    cache.update("AAPL", 190.0)
    await source.remove_ticker("AAPL")
    assert source.get_tickers() == []
    assert cache.get("AAPL") is None
