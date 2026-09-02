"""Seed prices and per-ticker parameters for the market simulator.

Configuration as data: this module imports nothing, so tuning the simulation
never means touching logic, and tests can import these constants to assert
against them.
"""

# Realistic starting prices for the default watchlist.
# Plausible-as-of-authoring, not current — they need to be recognisable,
# not accurate.
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters.
#   sigma: annualised volatility (higher = more price movement)
#   mu:    annualised drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Applied to any ticker a user adds that is not listed above.
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Price range used to seed an unknown ticker (uniform random).
UNKNOWN_PRICE_RANGE: tuple[float, float] = (50.0, 300.0)

# Correlation groups for the simulator's Cholesky decomposition.
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients. The minimum eigenvalue of the resulting matrix is
# 0.40 (= 1 - INTRA_TECH_CORR); re-check it if these values ever change, or
# np.linalg.cholesky will raise inside add_ticker().
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors, and for unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
