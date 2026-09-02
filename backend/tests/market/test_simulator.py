"""Statistical and structural tests for GBMSimulator.

The model is synchronous and takes injected RNGs, so these run in
milliseconds with no event loop, no sleeping and no mocking.
"""

from __future__ import annotations

import math
import random

import numpy as np
import pytest

from app.market.seed_prices import DEFAULT_PARAMS, SEED_PRICES, TICKER_PARAMS, UNKNOWN_PRICE_RANGE
from app.market.simulator import GBMSimulator

SEED = 42


def make_sim(tickers: list[str], seed: int = SEED, **kwargs) -> GBMSimulator:
    """A simulator with both RNG streams seeded — the numpy one drives the
    diffusion, the stdlib one drives shocks and unknown seed prices."""
    return GBMSimulator(
        tickers=tickers,
        rng=np.random.default_rng(seed),
        shock_rng=random.Random(seed),
        **kwargs,
    )


# --- Construction and state ---


def test_seeds_known_tickers_from_the_table():
    sim = make_sim(["AAPL", "NVDA"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]
    assert sim.get_price("NVDA") == SEED_PRICES["NVDA"]


def test_unknown_ticker_gets_a_random_price_and_default_params():
    sim = make_sim([])
    sim.add_ticker("ZZZZ")
    low, high = UNKNOWN_PRICE_RANGE
    assert low <= sim.get_price("ZZZZ") <= high
    assert sim._params["ZZZZ"] == DEFAULT_PARAMS


def test_default_params_are_copied_not_shared():
    """A mutation of one unknown ticker's params must not affect another's."""
    sim = make_sim(["AAAA", "BBBB"])
    sim._params["AAAA"]["sigma"] = 99.0
    assert sim._params["BBBB"]["sigma"] == DEFAULT_PARAMS["sigma"]
    assert DEFAULT_PARAMS["sigma"] == 0.25


def test_tickers_are_normalised():
    sim = make_sim(["aapl", " msft "])
    assert sim.get_tickers() == ["AAPL", "MSFT"]
    assert sim.get_price("aapl") == SEED_PRICES["AAPL"]


def test_get_price_returns_none_for_untracked_ticker():
    assert make_sim(["AAPL"]).get_price("NOPE") is None


def test_step_on_an_empty_simulator_returns_empty():
    assert make_sim([]).step() == {}


def test_add_ticker_is_a_noop_when_already_present():
    sim = make_sim(["AAPL"])
    sim.step()
    price = sim.get_price("AAPL")
    sim.add_ticker("AAPL")
    assert sim.get_tickers() == ["AAPL"]
    assert sim.get_price("AAPL") == price  # not re-seeded


def test_remove_ticker_is_a_noop_when_absent():
    sim = make_sim(["AAPL"])
    sim.remove_ticker("NOPE")
    assert sim.get_tickers() == ["AAPL"]


def test_remove_ticker_drops_all_state():
    sim = make_sim(["AAPL", "MSFT"])
    sim.remove_ticker("AAPL")
    assert sim.get_tickers() == ["MSFT"]
    assert sim.get_price("AAPL") is None
    assert "AAPL" not in sim.step()


# --- Determinism ---


def test_identical_seeds_produce_identical_paths():
    """Both streams must be seeded: seeding one alone is not enough."""
    a, b = make_sim(["AAPL", "MSFT"]), make_sim(["AAPL", "MSFT"])
    assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]


def test_different_seeds_produce_different_paths():
    a, b = make_sim(["AAPL"], seed=1), make_sim(["AAPL"], seed=2)
    assert [a.step() for _ in range(50)] != [b.step() for _ in range(50)]


# --- The maths ---


def test_prices_stay_positive_over_a_long_run():
    """Guaranteed by construction — this catches a regression to additive updates."""
    sim = make_sim(["AAPL"])
    for _ in range(100_000):
        sim.step()
    assert sim.get_price("AAPL") > 0


def test_realised_volatility_matches_sigma():
    """With shocks off, the sd of log returns should track sigma*sqrt(dt)."""
    sim = make_sim(["AAPL"], event_probability=0.0)
    prices = []
    for _ in range(50_000):
        sim.step()
        prices.append(sim.get_price("AAPL"))  # unrounded state: 2dp rounding
    log_returns = np.diff(np.log(prices))  # is ~24% of a one-tick move
    expected = TICKER_PARAMS["AAPL"]["sigma"] * math.sqrt(GBMSimulator.DEFAULT_DT)
    assert log_returns.std() == pytest.approx(expected, rel=0.05)


def test_higher_sigma_produces_more_movement():
    """TSLA (0.50) must visibly out-move JPM (0.18)."""
    sim = make_sim(["TSLA", "JPM"], event_probability=0.0)
    tsla, jpm = [], []
    for _ in range(20_000):
        sim.step()
        tsla.append(sim.get_price("TSLA"))
        jpm.append(sim.get_price("JPM"))
    assert np.diff(np.log(tsla)).std() > 2 * np.diff(np.log(jpm)).std()


def test_drift_has_no_sigma_squared_bias(monkeypatch):
    """Zero volatility: the realised log return must be exactly mu*dt*steps.

    Omitting the Ito correction (-sigma^2/2) would not show up here, but a
    sign or factor error in the drift term would.
    """
    monkeypatch.setitem(TICKER_PARAMS, "FLAT", {"sigma": 0.0, "mu": 0.10})
    sim = make_sim(["FLAT"], event_probability=0.0)
    start = sim.get_price("FLAT")
    steps = 10_000
    for _ in range(steps):
        sim.step()
    expected = start * math.exp(0.10 * GBMSimulator.DEFAULT_DT * steps)
    assert sim.get_price("FLAT") == pytest.approx(expected, rel=1e-9)


def test_ito_correction_is_applied(monkeypatch):
    """With sigma > 0 the drift term must be (mu - sigma^2/2)*dt, so the mean
    log return sits below mu*dt."""
    monkeypatch.setitem(TICKER_PARAMS, "VOL", {"sigma": 0.80, "mu": 0.10})
    sim = make_sim(["VOL"], event_probability=0.0)
    prices = []
    for _ in range(200_000):
        sim.step()
        prices.append(sim.get_price("VOL"))
    mean_log_return = np.diff(np.log(prices)).mean()
    dt = GBMSimulator.DEFAULT_DT
    expected = (0.10 - 0.5 * 0.80**2) * dt
    assert mean_log_return == pytest.approx(
        expected, abs=5 * 0.80 * math.sqrt(dt) / math.sqrt(200_000)
    )


def test_tech_tickers_are_correlated():
    sim = make_sim(["AAPL", "MSFT"], event_probability=0.0)
    aapl, msft = [], []
    for _ in range(50_000):
        sim.step()
        aapl.append(sim.get_price("AAPL"))
        msft.append(sim.get_price("MSFT"))
    corr = np.corrcoef(np.diff(np.log(aapl)), np.diff(np.log(msft)))[0, 1]
    assert corr == pytest.approx(0.6, abs=0.05)


def test_tsla_is_decoupled_from_tech():
    sim = make_sim(["AAPL", "TSLA"], event_probability=0.0)
    aapl, tsla = [], []
    for _ in range(50_000):
        sim.step()
        aapl.append(sim.get_price("AAPL"))
        tsla.append(sim.get_price("TSLA"))
    corr = np.corrcoef(np.diff(np.log(aapl)), np.diff(np.log(tsla)))[0, 1]
    assert corr == pytest.approx(0.3, abs=0.05)


@pytest.mark.parametrize(
    "pair,expected",
    [
        (("AAPL", "MSFT"), 0.6),
        (("JPM", "V"), 0.5),
        (("AAPL", "JPM"), 0.3),
        (("TSLA", "NVDA"), 0.3),
        (("ZZZZ", "AAPL"), 0.3),
        (("ZZZZ", "YYYY"), 0.3),
    ],
)
def test_pairwise_correlation_table(pair, expected):
    assert GBMSimulator._pairwise_correlation(*pair) == expected
    assert GBMSimulator._pairwise_correlation(*reversed(pair)) == expected


# --- Cholesky robustness ---


def test_cholesky_handles_the_full_default_watchlist():
    sim = make_sim(list(SEED_PRICES))
    assert len(sim.step()) == 10


def test_correlation_matrix_stays_positive_definite_with_many_unknowns():
    """add_ticker() rebuilds the factorisation; LinAlgError here would surface
    as a user-facing failure when someone adds a symbol."""
    sim = make_sim(list(SEED_PRICES))
    for i in range(40):
        sim.add_ticker(f"SYM{i}")
        sim.step()
    assert len(sim.get_tickers()) == 50
    assert np.linalg.eigvalsh(sim._cholesky @ sim._cholesky.T).min() == pytest.approx(0.4, abs=1e-9)


def test_cholesky_survives_add_remove_churn():
    sim = make_sim(["AAPL"])
    for symbol in ["ZZZZ", "TSLA", "JPM", "QQQQ", "V"]:
        sim.add_ticker(symbol)
        sim.step()
    for symbol in ["AAPL", "TSLA", "ZZZZ"]:
        sim.remove_ticker(symbol)
        sim.step()
    assert sorted(sim.get_tickers()) == ["JPM", "QQQQ", "V"]


def test_single_ticker_has_no_cholesky_factor():
    sim = make_sim(["AAPL"])
    assert sim._cholesky is None
    assert sim.step()["AAPL"] > 0


# --- Shock events ---


def test_shock_magnitude_stays_within_bounds(monkeypatch):
    """Forced shocks on a zero-volatility ticker: every move is 2-5%."""
    monkeypatch.setitem(TICKER_PARAMS, "FLAT", {"sigma": 0.0, "mu": 0.0})
    sim = make_sim(["FLAT"], event_probability=1.0)
    previous = sim.get_price("FLAT")
    for _ in range(500):
        sim.step()
        current = sim.get_price("FLAT")
        assert 0.02 - 1e-9 <= abs(current / previous - 1) <= 0.05 + 1e-9
        previous = current


def test_shocks_go_both_ways(monkeypatch):
    monkeypatch.setitem(TICKER_PARAMS, "FLAT", {"sigma": 0.0, "mu": 0.0})
    sim = make_sim(["FLAT"], event_probability=1.0)
    moves = []
    previous = sim.get_price("FLAT")
    for _ in range(200):
        sim.step()
        current = sim.get_price("FLAT")
        moves.append(current > previous)
        previous = current
    assert any(moves) and not all(moves)


def test_no_shocks_when_probability_is_zero(monkeypatch):
    """Zero vol, zero shocks: the price is pure drift."""
    monkeypatch.setitem(TICKER_PARAMS, "FLAT", {"sigma": 0.0, "mu": 0.0})
    sim = make_sim(["FLAT"], event_probability=0.0)
    start = sim.get_price("FLAT")
    for _ in range(1_000):
        sim.step()
    assert sim.get_price("FLAT") == pytest.approx(start, rel=1e-12)


# --- Emitted values ---


def test_step_emits_rounded_prices_but_keeps_full_precision_state():
    """State is unrounded, so sub-cent moves accumulate instead of vanishing."""
    sim = make_sim(["AAPL"])
    unrounded_state_seen = False
    for _ in range(50):
        emitted = sim.step()["AAPL"]
        assert emitted == round(emitted, 2)
        if sim.get_price("AAPL") != emitted:
            unrounded_state_seen = True
    assert unrounded_state_seen


def test_low_priced_tickers_do_not_freeze(monkeypatch):
    """Rounding the state would freeze a stock whose tick move is under half a cent."""
    monkeypatch.setitem(SEED_PRICES, "PENNY", 0.5)
    monkeypatch.setitem(TICKER_PARAMS, "PENNY", {"sigma": 0.30, "mu": 0.0})
    sim = make_sim(["PENNY"], event_probability=0.0)
    start = sim.get_price("PENNY")
    for _ in range(20_000):
        sim.step()
    assert sim.get_price("PENNY") != start


def test_step_returns_every_tracked_ticker():
    sim = make_sim(list(SEED_PRICES))
    assert set(sim.step()) == set(SEED_PRICES)


def test_duplicate_tickers_at_construction_are_collapsed():
    """The matrix index mapping assumes each ticker appears exactly once."""
    sim = make_sim(["AAPL", "aapl", "AAPL"])
    assert sim.get_tickers() == ["AAPL"]
    assert sim._cholesky is None
    assert set(sim.step()) == {"AAPL"}
