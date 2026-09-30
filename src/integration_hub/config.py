from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    log_level: str = "INFO"
    api_base_url: str = "http://localhost:9000"
    # Shared key required on every /v1 call (webhook endpoints excluded).
    service_api_key: str = ""

    database_url: str = "sqlite+aiosqlite:///./integration_hub.db"
    db_echo: bool = False

    # Fernet key used to encrypt provider credentials at rest.
    secret_encryption_key: str = ""

    scheduler_enabled: bool = True
    scheduler_poll_seconds: int = 30

    # Default guardrails applied to every provider unless it overrides them.
    default_page_size: int = 200
    default_write_batch_size: int = 100
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 5

    # ---- Zoho CRM ----
    zoho_client_id: str = ""
    zoho_client_secret: str = ""
    zoho_redirect_uri: str = "http://localhost:9000/v1/oauth/zoho_crm/callback"
    zoho_default_dc: str = "com"
    zoho_api_version: str = "v8"
    zoho_webhook_token: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
