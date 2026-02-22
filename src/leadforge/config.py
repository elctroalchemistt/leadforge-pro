from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # provider
    SCRAPER_PROVIDER: str = "mock"   # mock | places_api
    GOOGLE_API_KEY: str | None = None

    # rate limiting + http
    RPS: float = 5.0
    HTTP_TIMEOUT: float = 12.0
    HTTP_RETRIES: int = 3
    HTTP_BACKOFF_BASE: float = 0.7  # exponential backoff base seconds

    # concurrency
    MAX_CONCURRENCY_DETAILS: int = 8
    MAX_CONCURRENCY_EMAIL: int = 8

    # caching (for Places API / expensive calls)
    CACHE_ENABLED: bool = True
    CACHE_DIR: str = ".cache"

    # scoring weights
    SCORE_WEBSITE: int = 1
    SCORE_EMAIL: int = 2
    SCORE_RATING_GT_4: int = 1
    SCORE_REVIEWS_GT_50: int = 1
    SCORE_PHONE: int = 1

    # thresholds
    HOT_THRESHOLD: int = 5
    WARM_THRESHOLD: int = 3


settings = Settings()