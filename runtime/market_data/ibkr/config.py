from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class IBKRSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IBKR_", env_file=".env", extra="ignore")

    host: str = "127.0.0.1"
    port: int = 7497       # 7497 = TWS paper, 7496 = TWS live, 4001/4002 = Gateway
    client_id: int = 1
    timeout: float = 20.0

    # L1 quote streaming (reqMktData ticker)
    stream_quotes: bool = True

    # L2 market depth streaming (reqMktDepth)
    # Requires a market-data subscription that includes Level II.
    # When False the system runs in L1-only mode — MicrostructureEngine
    # still computes spread / mid / bid_ask_imbalance from L1 quotes.
    stream_depth: bool = False

    # Number of Level II rows requested (each side)
    depth_rows: int = 5


@lru_cache
def get_ibkr_settings() -> IBKRSettings:
    return IBKRSettings()
