from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class IBKRSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IBKR_", env_file=".env", extra="ignore")

    host: str = "127.0.0.1"
    port: int = 7497       # 7497 = TWS paper, 7496 = TWS live, 4001/4002 = Gateway
    client_id: int = 1
    timeout: float = 20.0


@lru_cache
def get_ibkr_settings() -> IBKRSettings:
    return IBKRSettings()
