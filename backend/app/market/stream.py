"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

STREAM_INTERVAL = 0.5  # How often to check the cache for changes
HEARTBEAT_INTERVAL = 15.0  # Max silence before sending a comment frame


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build the SSE router bound to a specific PriceCache.

    A factory rather than a module-level router: the cache is injected without
    globals, and calling this twice (as tests naturally do) yields two
    independent routers instead of registering /prices twice on one.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """Live price stream. Consume with `new EventSource('/api/stream/prices')`."""
        return StreamingResponse(
            price_event_stream(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Defeat nginx response buffering
            },
        )

    return router


async def price_event_stream(
    price_cache: PriceCache,
    request: Request,
    interval: float = STREAM_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects.

    Sends the full price map whenever the cache version changes, and a comment
    frame during long silences so idle proxies do not close the connection.

    Frames are unnamed `data:` events, so the browser fires its default
    `message` event and `es.onmessage` works without addEventListener.
    """
    # Browser auto-reconnect delay. EventSource handles reconnection itself.
    yield "retry: 1000\n\n"

    last_version = -1
    last_emit = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    try:
        while True:
            # Without this a browser that closed its tab would leave the
            # generator ticking forever.
            if await request.is_disconnected():
                break

            now = time.monotonic()
            version = price_cache.version

            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                    yield f"data: {payload}\n\n"
                    last_emit = now
            elif now - last_emit >= heartbeat:
                # A comment frame: keeps the socket warm under the Massive
                # source, which changes nothing for 15 seconds at a time.
                yield ": keepalive\n\n"
                last_emit = now

            await asyncio.sleep(interval)
    finally:
        logger.info("SSE client disconnected: %s", client)
