"""Tests for the SSE streaming endpoint."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from app.market import PriceCache, create_stream_router
from app.market.stream import price_event_stream


class StubRequest:
    """Minimal stand-in for a Starlette Request.

    Reports connected for `alive` polls, then disconnected — which is how the
    generator is made to terminate without a real client.
    """

    def __init__(self, alive: int = 1, host: str | None = "test-client") -> None:
        self._polls = 0
        self._alive = alive
        self.client = SimpleNamespace(host=host) if host else None

    async def is_disconnected(self) -> bool:
        self._polls += 1
        return self._polls > self._alive


async def frames(cache: PriceCache, request: StubRequest, **kwargs) -> list[str]:
    return [frame async for frame in price_event_stream(cache, request, interval=0.0, **kwargs)]


# --- Frame format ---


async def test_first_frame_sets_the_reconnect_delay(cache: PriceCache):
    assert (await frames(cache, StubRequest(alive=0)))[0] == "retry: 1000\n\n"


async def test_emits_the_full_price_map_as_one_data_frame(cache: PriceCache):
    cache.update("AAPL", 190.0)
    cache.update("MSFT", 420.0)
    emitted = await frames(cache, StubRequest(alive=1))
    payload = json.loads(emitted[1].removeprefix("data: ").rstrip("\n"))
    assert set(payload) == {"AAPL", "MSFT"}
    assert payload["AAPL"]["price"] == 190.0
    assert payload["AAPL"]["direction"] == "flat"


async def test_frames_are_unnamed_so_onmessage_receives_them(cache: PriceCache):
    """A named event would need addEventListener on the client."""
    cache.update("AAPL", 190.0)
    data = [f for f in await frames(cache, StubRequest(alive=1)) if f.startswith("data:")]
    assert data and not any(f.startswith("event:") for f in await frames(cache, StubRequest(1)))
    assert data[0].endswith("\n\n")


async def test_nothing_is_sent_while_the_cache_is_empty(cache: PriceCache):
    assert await frames(cache, StubRequest(alive=3)) == ["retry: 1000\n\n"]


# --- Change detection ---


async def test_resends_only_when_the_version_changes(cache: PriceCache):
    cache.update("AAPL", 190.0)
    emitted = await frames(cache, StubRequest(alive=5), heartbeat=1e9)
    assert len([f for f in emitted if f.startswith("data:")]) == 1


async def test_a_new_price_produces_a_new_frame(cache: PriceCache):
    cache.update("AAPL", 190.0)

    async def bump() -> None:
        await asyncio.sleep(0)
        cache.update("AAPL", 191.0)

    request = StubRequest(alive=6)
    task = asyncio.create_task(bump())
    emitted = [
        frame async for frame in price_event_stream(cache, request, interval=0.001, heartbeat=1e9)
    ]
    await task
    data = [json.loads(f.removeprefix("data: ").rstrip("\n")) for f in emitted if f[0] == "d"]
    assert [d["AAPL"]["price"] for d in data] == [190.0, 191.0]
    assert data[-1]["AAPL"]["direction"] == "up"


async def test_removed_tickers_disappear_from_the_payload(cache: PriceCache):
    cache.update("AAPL", 190.0)
    cache.update("MSFT", 420.0)
    cache.remove("AAPL")
    emitted = await frames(cache, StubRequest(alive=1))
    assert set(json.loads(emitted[1].removeprefix("data: ").rstrip("\n"))) == {"MSFT"}


# --- Heartbeat and disconnect ---


async def test_heartbeat_keeps_an_idle_connection_warm(cache: PriceCache):
    """Under Massive nothing changes for 15s at a stretch; idle proxies close
    connections with no traffic."""
    cache.update("AAPL", 190.0)
    emitted = await frames(cache, StubRequest(alive=3), heartbeat=0.0)
    assert emitted.count(": keepalive\n\n") == 2  # every poll after the data frame


async def test_no_heartbeat_while_prices_are_flowing(cache: PriceCache):
    cache.update("AAPL", 190.0)
    emitted = await frames(cache, StubRequest(alive=2), heartbeat=1e9)
    assert ": keepalive\n\n" not in emitted


async def test_generator_stops_when_the_client_disconnects(cache: PriceCache):
    """Without this a closed tab leaves the generator ticking forever."""
    cache.update("AAPL", 190.0)
    request = StubRequest(alive=2)
    await frames(cache, request)
    assert request._polls == 3  # stopped on the first disconnected poll


async def test_missing_client_host_does_not_break_logging(cache: PriceCache):
    assert await frames(cache, StubRequest(alive=0, host=None)) == ["retry: 1000\n\n"]


# --- Router wiring ---


def test_router_is_built_per_call(cache: PriceCache):
    """A module-level router would register /prices twice on the second call."""
    first = create_stream_router(cache)
    second = create_stream_router(cache)
    assert first is not second
    assert [r.path for r in first.routes] == ["/api/stream/prices"]
    assert [r.path for r in second.routes] == ["/api/stream/prices"]


async def test_endpoint_returns_a_streaming_sse_response(cache: PriceCache):
    """Exercises the real route: media type, headers, and a live body iterator.

    Note this calls the endpoint directly rather than going through
    httpx.ASGITransport — that transport buffers the entire response body
    before returning, so an endless stream never yields a response object.
    """
    cache.update("AAPL", 190.0)
    app = FastAPI()
    app.include_router(create_stream_router(cache))
    endpoint = next(r for r in app.routes if getattr(r, "path", None) == "/api/stream/prices")

    response = await endpoint.endpoint(request=StubRequest(alive=1))

    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"  # nginx would buffer otherwise

    body = [chunk async for chunk in response.body_iterator]
    assert body[0] == "retry: 1000\n\n"
    payload = json.loads(body[1].removeprefix("data: ").rstrip("\n"))
    assert payload["AAPL"]["price"] == 190.0


@pytest.mark.parametrize("clients", [3])
async def test_multiple_clients_each_get_their_own_stream(cache: PriceCache, clients: int):
    cache.update("AAPL", 190.0)
    results = await asyncio.gather(*(frames(cache, StubRequest(alive=1)) for _ in range(clients)))
    assert all(len(r) == 2 and r[1].startswith("data:") for r in results)
