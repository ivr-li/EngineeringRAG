import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    database_url: str
    session_ttl_days: int


@lru_cache
def get_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "USER_API_DATABASE_URL",
            "postgresql+asyncpg://app_user:app_password@localhost:5432/app_db",
        ),
        session_ttl_days=_read_int("USER_API_SESSION_TTL_DAYS", 30),
    )


def _read_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    return int(raw_value)
