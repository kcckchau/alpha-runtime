from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class RiskSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RISK_", env_file=".env", extra="ignore")

    paper_mode: bool = True
    rth_only: bool = True
    min_rr: float = 2.0
    max_daily_loss: float = 500.0    # USD
    risk_per_trade: float = 100.0    # USD risk per trade
    max_shares: float = 500.0        # hard cap on position size
    auto_approve: bool = False       # False = require human approval via API
    signal_expiry_minutes: int = 5   # PENDING_APPROVAL signals expire after this many minutes


@lru_cache
def get_risk_settings() -> RiskSettings:
    return RiskSettings()
