"""
Bootstrap / historical backfill configuration.

All settings can be overridden via environment variables prefixed with
BOOTSTRAP_ (e.g. BOOTSTRAP_LOOKBACK_DAYS=2).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class BootstrapSettings(BaseSettings):
    model_config = {"env_prefix": "BOOTSTRAP_", "extra": "ignore"}

    # Whether to run historical backfill on startup at all
    enabled: bool = True

    # Number of prior *trading* days of RTH data to fetch
    lookback_days: int = 3

    # Suppress strategy signals from historical bars (default: True)
    # Set to False only when intentionally replaying a past session
    suppress_historical_signals: bool = True

    # Premarket start hour/minute (Eastern Time)
    premarket_start_hour: int = 4
    premarket_start_minute: int = 0

    # IBKR bar size for historical requests
    bar_size: str = "1 min"

    # What-to-show for historical requests
    what_to_show: str = "TRADES"
