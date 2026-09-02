"""Tests for PriceUpdate."""

from __future__ import annotations

import dataclasses
import time

import pytest

from app.market import PriceUpdate


def test_change_is_absolute_difference():
    assert PriceUpdate("AAPL", 191.0, 190.0).change == 1.0
    assert PriceUpdate("AAPL", 189.0, 190.0).change == -1.0


def test_change_percent():
    assert PriceUpdate("AAPL", 209.0, 190.0).change_percent == 10.0
    assert PriceUpdate("AAPL", 171.0, 190.0).change_percent == -10.0


def test_change_percent_guards_against_zero_previous():
    """A zero previous price must not raise ZeroDivisionError."""
    assert PriceUpdate("AAPL", 10.0, 0.0).change_percent == 0.0


@pytest.mark.parametrize(
    "price,previous,expected",
    [(191.0, 190.0, "up"), (189.0, 190.0, "down"), (190.0, 190.0, "flat")],
)
def test_direction(price, previous, expected):
    assert PriceUpdate("AAPL", price, previous).direction == expected


def test_timestamp_defaults_to_now():
    before = time.time()
    update = PriceUpdate("AAPL", 190.0, 190.0)
    assert before <= update.timestamp <= time.time()


def test_is_immutable():
    """Frozen: one cached object is handed to many readers concurrently."""
    update = PriceUpdate("AAPL", 190.0, 189.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        update.price = 200.0  # type: ignore[misc]


def test_uses_slots():
    """slots=True: 20 of these are created per second, indefinitely."""
    update = PriceUpdate("AAPL", 190.0, 189.0)
    with pytest.raises(AttributeError):
        update.extra_field = 1  # type: ignore[attr-defined]


def test_to_dict_contains_the_full_sse_payload():
    payload = PriceUpdate("AAPL", 191.0, 190.0, timestamp=1_700_000_000.0).to_dict()
    assert payload == {
        "ticker": "AAPL",
        "price": 191.0,
        "previous_price": 190.0,
        "timestamp": 1_700_000_000.0,
        "change": 1.0,
        "change_percent": 0.5263,
        "direction": "up",
    }


def test_to_dict_is_json_serialisable():
    import json

    assert json.loads(json.dumps(PriceUpdate("AAPL", 1.0, 1.0).to_dict()))["ticker"] == "AAPL"


def test_derived_values_cannot_drift_from_price():
    """change/direction are properties, so they are always consistent."""
    update = PriceUpdate("AAPL", 190.0, 190.0)
    assert update.change == 0.0 and update.direction == "flat"
