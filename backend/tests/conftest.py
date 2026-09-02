"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import random

import numpy as np
import pytest

from app.market import PriceCache

# A fixed seed used wherever a test needs a reproducible price path.
SEED = 42


@pytest.fixture
def cache() -> PriceCache:
    """An empty PriceCache."""
    return PriceCache()


@pytest.fixture
def rng() -> np.random.Generator:
    """Seeded numpy generator for the GBM diffusion."""
    return np.random.default_rng(SEED)


@pytest.fixture
def shock_rng() -> random.Random:
    """Seeded stdlib generator for shock events and unknown seed prices.

    The simulator draws from two independent streams, so a test that seeds
    only one of them is still non-deterministic.
    """
    return random.Random(SEED)


@pytest.fixture(autouse=True)
def _clear_market_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep a developer's real .env out of the test run.

    Without this, anyone with MASSIVE_API_KEY exported would have the factory
    tests build a live client.
    """
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_POLL_INTERVAL", raising=False)
