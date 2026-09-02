"""Tests for the market data source factory and the Massive fallback wrapper."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app.market import PriceCache, create_market_data_source
from app.market.factory import (
    DEFAULT_POLL_INTERVAL,
    MassiveWithSimulatorFallback,
    _poll_interval_from_env,
)
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource

# --- Source selection ---


def test_simulator_when_the_key_is_unset(cache: PriceCache):
    assert isinstance(create_market_data_source(cache), SimulatorDataSource)


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_simulator_when_the_key_is_blank(cache: PriceCache, monkeypatch, value):
    """MASSIVE_API_KEY= in a .env means "not set". Without .strip() a stray
    space would route to a live client that then fails auth."""
    monkeypatch.setenv("MASSIVE_API_KEY", value)
    assert isinstance(create_market_data_source(cache), SimulatorDataSource)


def test_massive_when_the_key_is_set(cache: PriceCache, monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-real-key")
    source = create_market_data_source(cache)
    assert isinstance(source, MassiveWithSimulatorFallback)
    assert isinstance(source.active, MassiveDataSource)


def test_surrounding_whitespace_is_stripped_from_the_key(cache: PriceCache, monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "  sk-real-key  ")
    source = create_market_data_source(cache)
    assert source.active._api_key == "sk-real-key"


def test_the_returned_source_is_unstarted(cache: PriceCache):
    """Construction has no side effects; the caller owns start()."""
    source = create_market_data_source(cache)
    assert source.get_tickers() == []
    assert len(cache) == 0


def test_the_chosen_source_is_logged(cache: PriceCache, caplog):
    """'Why am I seeing fake prices?' should be answered in the container log."""
    with caplog.at_level("INFO"):
        create_market_data_source(cache)
    assert "GBM Simulator" in caplog.text


# --- Poll interval ---


def test_poll_interval_defaults(cache: PriceCache):
    assert _poll_interval_from_env() == DEFAULT_POLL_INTERVAL


@pytest.mark.parametrize("raw,expected", [("2", 2.0), ("7.5", 7.5), (" 30 ", 30.0)])
def test_poll_interval_is_read_from_the_environment(monkeypatch, raw, expected):
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", raw)
    assert _poll_interval_from_env() == expected


@pytest.mark.parametrize("raw", ["abc", "-5", "0"])
def test_invalid_poll_interval_falls_back_to_the_default(monkeypatch, raw, caplog):
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", raw)
    assert _poll_interval_from_env() == DEFAULT_POLL_INTERVAL
    assert "MASSIVE_POLL_INTERVAL" in caplog.text


def test_poll_interval_reaches_the_client(cache: PriceCache, monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "3")
    assert create_market_data_source(cache).active._interval == 3.0


# --- Fallback wrapper ---


@pytest.fixture
def fallback(cache: PriceCache) -> MassiveWithSimulatorFallback:
    return MassiveWithSimulatorFallback(api_key="k", price_cache=cache, poll_interval=0.01)


async def test_delegates_to_massive_while_it_works(fallback, cache: PriceCache):
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(fallback.active, "_fetch_snapshots", return_value=[]),
    ):
        await fallback.start([" aapl ", "MSFT"])
        assert fallback.get_tickers() == ["AAPL", "MSFT"]
        assert isinstance(fallback.active, MassiveDataSource)
        await fallback.stop()


async def test_switches_to_the_simulator_on_a_terminal_error(fallback, cache: PriceCache, caplog):
    """A free-tier key 403s forever; showing a dead watchlist is not an option."""
    from massive.exceptions import BadResponse

    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(
            fallback.active,
            "_fetch_snapshots",
            side_effect=BadResponse('{"status":"NOT_AUTHORIZED"}'),
        ),
    ):
        await fallback.start(["AAPL", "MSFT"])

    # on_fatal is scheduled as its own task so the poll task can finish first.
    await asyncio.sleep(0.05)

    assert isinstance(fallback.active, SimulatorDataSource)
    assert "Falling back to the simulator" in caplog.text
    assert cache.get_price("AAPL") is not None  # the simulator seeded it
    assert fallback.get_tickers() == ["AAPL", "MSFT"]
    await fallback.stop()


async def test_tickers_added_before_the_switch_survive_it(fallback, cache: PriceCache):
    from massive.exceptions import AuthError

    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(fallback.active, "_fetch_snapshots", side_effect=AuthError("bad key")),
    ):
        await fallback.start(["AAPL"])
        await fallback.add_ticker("tsla")

    await asyncio.sleep(0.05)
    assert isinstance(fallback.active, SimulatorDataSource)
    assert fallback.get_tickers() == ["AAPL", "TSLA"]
    assert cache.get_price("TSLA") is not None
    await fallback.stop()


async def test_add_and_remove_are_mirrored_to_the_active_source(fallback, cache: PriceCache):
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(fallback.active, "_fetch_snapshots", return_value=[]),
    ):
        await fallback.start(["AAPL"])
        await fallback.add_ticker("msft")
        assert fallback.active.get_tickers() == ["AAPL", "MSFT"]
        await fallback.remove_ticker("AAPL")
        assert fallback.get_tickers() == ["MSFT"]
        assert fallback.active.get_tickers() == ["MSFT"]
        await fallback.stop()


async def test_add_existing_ticker_is_not_duplicated(fallback):
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(fallback.active, "_fetch_snapshots", return_value=[]),
    ):
        await fallback.start(["AAPL"])
        await fallback.add_ticker("AAPL")
        assert fallback.get_tickers() == ["AAPL"]
        await fallback.stop()
