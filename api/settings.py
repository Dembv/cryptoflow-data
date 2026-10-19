"""Configuration de l'API, validée au démarrage (variables d'environnement ou config/.env)."""

from functools import lru_cache
from typing import List, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="config/.env", env_file_encoding="utf-8", extra="ignore")

    assets: str = "BTC,ETH,SOL,BNB,XRP"

    timescale_host: str = "localhost"
    timescale_port: int = 5433
    timescale_db: str = "cryptoflow"
    timescale_user: str = "crypto"
    timescale_password: str = "crypto"
    db_pool_max: int = 10

    redis_host: str = "localhost"
    redis_port: int = 6379

    kafka_bootstrap_servers: str = "localhost:9092"

    api_key: Optional[str] = None          # vide = pas d'authentification
    cors_origins: str = "*"
    rate_limit_per_min: int = 120          # 0 = désactivé
    cache_ttl_summary: int = 10            # secondes
    ws_interval_sec: float = 1.0

    @field_validator("assets")
    @classmethod
    def _assets_not_empty(cls, value: str) -> str:
        if not [a for a in value.split(",") if a.strip()]:
            raise ValueError("ASSETS ne peut pas être vide (ex. BTC,ETH,SOL)")
        return value

    @field_validator("api_key")
    @classmethod
    def _blank_key_is_none(cls, value: Optional[str]) -> Optional[str]:
        return value or None

    @field_validator("rate_limit_per_min", "db_pool_max")
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("doit être >= 0")
        return value

    @property
    def asset_list(self) -> List[str]:
        return [a.strip().upper() for a in self.assets.split(",") if a.strip()]

    @property
    def cors_list(self) -> List[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
