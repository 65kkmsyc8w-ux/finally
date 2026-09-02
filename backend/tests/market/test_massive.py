"""Tests for MassiveDataSource.

Fixtures are built with UniversalSnapshot.from_dict() rather than Mock: a bare
Mock answers to *any* attribute, so a test asserting on `snap.last_trade.timestamp`
passes against a client that has no such field. That is exactly how the
nanosecond/millisecond defect survived a green suite. Real model objects turn a
wrong field name into a failure.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import UniversalSnapshot

from app.market import PriceCache
from app.market.massive_client import (
    SNAPSHOT_LIMIT,
    MassiveDataSource,
    extract_price,
    is_terminal_error,
)

NANOS = 1_675_190_399_000_000_000  # 2023-01-31T18:39:59Z


def snapshot(ticker: str = "AAPL", price: float | None = 190.0, ts: int | None = NANOS, **extra):
    """A real UniversalSnapshot, built the way the client builds one from JSON."""
    payload: dict = {"ticker": ticker, "type": "stocks", "market_status": "open"}
    if price is not None:
        payload["last_trade"] = {"price": price, "size": 100, "sip_timestamp": ts}
        payload["session"] = {"price": price, "previous_close": price - 1.0}
    payload.update(extra)
    return UniversalSnapshot.from_dict(payload)


@pytest.fixture
def source(cache: PriceCache) -> MassiveDataSource:
    """A source wired to a cache, with no client and no background task."""
    src = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=0.01)
    src._client = object()  # non-None so _poll_once proceeds; never called
    return src


# --- Price extraction: the field names ---


def test_extracts_price_and_converts_nanoseconds_to_seconds():
    """sip_timestamp is nanoseconds; PriceCache wants seconds. Dividing by
    1000 instead of 1e9 lands the timestamp in the year 54,000."""
    assert extract_price(snapshot(price=190.42)) == (190.42, 1_675_190_399.0)


def test_falls_back_to_participant_timestamp():
    snap = UniversalSnapshot.from_dict(
        {"ticker": "AAPL", "last_trade": {"price": 190.0, "participant_timestamp": NANOS}}
    )
    assert extract_price(snap) == (190.0, 1_675_190_399.0)


def test_price_without_a_timestamp_yields_none_timestamp():
    """The cache then stamps it with now(), which is the honest answer."""
    snap = UniversalSnapshot.from_dict({"ticker": "AAPL", "last_trade": {"price": 190.0}})
    assert extract_price(snap) == (190.0, None)


def test_falls_back_to_session_price_when_there_is_no_trade():
    """Outside market hours v3 still carries a session price."""
    snap = UniversalSnapshot.from_dict(
        {"ticker": "AAPL", "session": {"price": 188.5, "previous_close": 190.0}}
    )
    assert extract_price(snap) == (188.5, None)


def test_falls_back_to_previous_close_as_a_last_resort():
    snap = UniversalSnapshot.from_dict({"ticker": "AAPL", "session": {"previous_close": 190.0}})
    assert extract_price(snap) == (190.0, None)


def test_returns_none_when_no_price_is_available():
    """Never guess — a fabricated fill price corrupts the portfolio permanently."""
    assert extract_price(UniversalSnapshot.from_dict({"ticker": "AAPL"})) is None
    assert extract_price(UniversalSnapshot.from_dict({"ticker": "AAPL", "session": {}})) is None


def test_per_ticker_error_is_reported_not_priced(caplog):
    snap = UniversalSnapshot.from_dict(
        {"ticker": "ZZZZ", "error": "NOT_FOUND", "message": "Ticker not found."}
    )
    assert extract_price(snap) is None
    assert "ZZZZ" in caplog.text


def test_universal_snapshot_has_no_top_level_last_updated():
    """Guards the doc claim this design corrects: timestamps live on last_trade."""
    assert not hasattr(snapshot(), "last_updated")
    assert hasattr(snapshot().last_trade, "sip_timestamp")
    assert not hasattr(snapshot().session, "last_updated")


# --- Error classification ---


@pytest.mark.parametrize(
    "body,terminal",
    [
        ('{"status":"NOT_AUTHORIZED","message":"You are not entitled to this data."}', True),
        ('{"status":"ERROR","message":"Unknown API Key"}', True),
        ('{"message":"Please upgrade your plan at https://massive.com/pricing"}', True),
        ('{"status":"ERROR","message":"Too Many Requests"}', False),
        ("upstream connect error", False),
        ("", False),
    ],
)
def test_error_classification_reads_the_response_body(body, terminal):
    """BadResponse carries no status code — only the decoded body."""
    assert is_terminal_error(BadResponse(body)) is terminal


def test_auth_error_is_terminal():
    assert is_terminal_error(AuthError("no key")) is True


def test_unrelated_exceptions_are_transient():
    assert is_terminal_error(TimeoutError()) is False
    assert is_terminal_error(ConnectionResetError()) is False


# --- Polling ---


async def test_poll_writes_every_price_to_the_cache(source, cache: PriceCache):
    source._tickers = ["AAPL", "MSFT"]
    with patch.object(
        source,
        "_fetch_snapshots",
        return_value=[snapshot("AAPL", 190.0), snapshot("MSFT", 420.0)],
    ):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.0
    assert cache.get_price("MSFT") == 420.0
    assert cache.get("AAPL").timestamp == pytest.approx(1_675_190_399.0)


async def test_poll_normalises_returned_tickers(source, cache: PriceCache):
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", return_value=[snapshot("aapl", 190.0)]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.0


async def test_poll_skips_unusable_snapshots_but_keeps_the_rest(source, cache: PriceCache):
    source._tickers = ["AAPL", "ZZZZ"]
    bad = UniversalSnapshot.from_dict({"ticker": "ZZZZ", "error": "NOT_FOUND"})
    with patch.object(source, "_fetch_snapshots", return_value=[snapshot("AAPL", 190.0), bad]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.0
    assert cache.get("ZZZZ") is None


async def test_poll_is_a_noop_with_no_tickers(source, cache: PriceCache):
    source._tickers = []
    with patch.object(source, "_fetch_snapshots") as fetch:
        await source._poll_once()
    fetch.assert_not_called()
    assert len(cache) == 0


async def test_poll_is_a_noop_without_a_client(cache: PriceCache):
    src = MassiveDataSource(api_key="k", price_cache=cache)
    src._tickers = ["AAPL"]
    with patch.object(src, "_fetch_snapshots") as fetch:
        await src._poll_once()
    fetch.assert_not_called()


async def test_transient_failure_leaves_the_cache_intact(source, cache: PriceCache, caplog):
    """Stale prices beat an empty watchlist."""
    cache.update("AAPL", 190.0)
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", side_effect=BadResponse("Too Many Requests")):
        await source._poll_once()  # must not raise
    assert cache.get_price("AAPL") == 190.0
    assert source.degraded is False
    assert "will retry" in caplog.text


async def test_network_failure_is_transient(source, cache: PriceCache):
    source._tickers = ["AAPL"]
    with patch.object(source, "_fetch_snapshots", side_effect=ConnectionError("dns")):
        await source._poll_once()
    assert source.degraded is False


async def test_terminal_failure_stops_the_poller(source, caplog):
    source._tickers = ["AAPL"]
    with patch.object(
        source, "_fetch_snapshots", side_effect=BadResponse('{"status":"NOT_AUTHORIZED"}')
    ):
        await source._poll_once()
    assert source.degraded is True
    assert "stopping poller" in caplog.text


async def test_terminal_failure_invokes_on_fatal_once(cache: PriceCache):
    reasons: list[str] = []

    async def on_fatal(reason: str) -> None:
        reasons.append(reason)

    src = MassiveDataSource(api_key="k", price_cache=cache, on_fatal=on_fatal)
    src._client = object()
    src._tickers = ["AAPL"]
    with patch.object(src, "_fetch_snapshots", side_effect=AuthError("bad key")):
        await src._poll_once()
        await src._poll_once()  # already degraded — must not fire again
    await asyncio.sleep(0)  # let the scheduled task run
    assert len(reasons) == 1


async def test_poll_snapshots_the_ticker_list(source):
    """The worker thread must not see a list mutated mid-request."""
    source._tickers = ["AAPL"]
    seen: list[list[str]] = []

    def fetch(tickers):
        seen.append(tickers)
        return [snapshot("AAPL", 190.0)]

    with patch.object(source, "_fetch_snapshots", side_effect=fetch):
        await source._poll_once()
    seen[0].append("MUTATED")
    assert source.get_tickers() == ["AAPL"]


# --- Lifecycle ---


async def test_start_polls_immediately_then_loops(cache: PriceCache):
    """The cache must be populated before start() returns."""
    src = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=0.01)
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(src, "_fetch_snapshots", return_value=[snapshot("AAPL", 190.0)]),
    ):
        await src.start(["aapl"])
        assert cache.get_price("AAPL") == 190.0  # before any sleep
        version = cache.version
        await asyncio.sleep(0.05)
        assert cache.version > version  # and the loop keeps polling
        await src.stop()


async def test_start_normalises_and_deduplicates(cache: PriceCache):
    src = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=0.01)
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(src, "_fetch_snapshots", return_value=[]),
    ):
        await src.start([" aapl ", "AAPL", "msft"])
        assert src.get_tickers() == ["AAPL", "MSFT"]
        await src.stop()


async def test_stop_cancels_the_task_and_is_idempotent(cache: PriceCache):
    src = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=0.01)
    with (
        patch("app.market.massive_client.RESTClient"),
        patch.object(src, "_fetch_snapshots", return_value=[]),
    ):
        await src.start(["AAPL"])
        task = src._task
        await src.stop()
        await src.stop()
    assert task.cancelled() or task.done()
    assert src._client is None


async def test_stop_without_start_is_safe(cache: PriceCache):
    await MassiveDataSource(api_key="k", price_cache=cache).stop()


async def test_add_ticker_appears_on_the_next_poll(source, cache: PriceCache):
    """Eventually consistent: Massive cannot seed a price instantly."""
    source._tickers = ["AAPL"]
    await source.add_ticker("tsla")
    assert source.get_tickers() == ["AAPL", "TSLA"]
    assert cache.get("TSLA") is None  # normal transient state, not an error

    with patch.object(
        source, "_fetch_snapshots", return_value=[snapshot("AAPL", 1.0), snapshot("TSLA", 250.0)]
    ):
        await source._poll_once()
    assert cache.get_price("TSLA") == 250.0


async def test_add_existing_ticker_is_a_noop(source):
    source._tickers = ["AAPL"]
    await source.add_ticker("AAPL")
    assert source.get_tickers() == ["AAPL"]


async def test_remove_ticker_evicts_from_the_cache(source, cache: PriceCache):
    cache.update("AAPL", 190.0)
    source._tickers = ["AAPL", "MSFT"]
    await source.remove_ticker("aapl")
    assert source.get_tickers() == ["MSFT"]
    assert cache.get("AAPL") is None


async def test_remove_unknown_ticker_is_a_noop(source):
    source._tickers = ["AAPL"]
    await source.remove_ticker("NOPE")
    assert source.get_tickers() == ["AAPL"]


# --- The request itself ---


async def test_fetch_requests_all_tickers_in_one_call_with_an_explicit_limit(cache: PriceCache):
    """limit defaults to 10 on this endpoint: without an explicit value a
    watchlist of 12 silently loses two tickers."""
    src = MassiveDataSource(api_key="k", price_cache=cache)
    with patch("app.market.massive_client.RESTClient") as client_cls:
        client = client_cls.return_value
        client.list_universal_snapshots.return_value = iter([snapshot("AAPL", 190.0)])
        src._client = client
        src._fetch_snapshots(["AAPL", "MSFT"])
    client.list_universal_snapshots.assert_called_once_with(
        type="stocks", ticker_any_of=["AAPL", "MSFT"], limit=SNAPSHOT_LIMIT
    )
    assert SNAPSHOT_LIMIT == 250


async def test_the_blocking_client_runs_off_the_event_loop(cache: PriceCache):
    """urllib3 is synchronous; calling it on the loop would stall every SSE stream."""
    import threading

    src = MassiveDataSource(api_key="k", price_cache=cache)
    src._client = object()
    src._tickers = ["AAPL"]
    calling_thread: list[int] = []

    def fetch(tickers):
        calling_thread.append(threading.get_ident())
        return [snapshot("AAPL", 190.0)]

    with patch.object(src, "_fetch_snapshots", side_effect=fetch):
        await src._poll_once()
    assert calling_thread[0] != threading.get_ident()
