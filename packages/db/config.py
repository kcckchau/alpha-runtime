from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://alpha:alpha@localhost:5432/alpha_runtime"
    echo_sql: bool = False
    pool_size: int = 5
    max_overflow: int = 10


@lru_cache
def get_db_settings() -> DBSettings:
    return DBSettings()
