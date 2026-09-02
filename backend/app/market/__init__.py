"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    normalize_ticker          - Canonical ticker form (uppercase, stripped)
    create_market_data_source - Factory selecting simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint

Consumers import from `app.market`, never from `app.market.simulator` or
`app.market.massive_client` — nothing downstream of the cache should be able
to tell which source is running.
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource, normalize_ticker
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "normalize_ticker",
    "create_market_data_source",
    "create_stream_router",
]
