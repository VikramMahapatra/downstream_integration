from __future__ import annotations

import os

os.environ.setdefault("SECRET_ENCRYPTION_KEY", "Zx4nKZ4Kc0dzZ0hVQmZ0WW1nUXhVZG1yTlZmYjFxYlk=")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test_integration_hub.db")
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("SCHEDULER_ENABLED", "false")
os.environ.setdefault("ZOHO_WEBHOOK_TOKEN", "")
os.environ.setdefault("ZOHO_CLIENT_ID", "test-client-id")
os.environ.setdefault("ZOHO_CLIENT_SECRET", "test-client-secret")

import pytest

from integration_hub.core.base import ConnectionContext


@pytest.fixture
def ctx() -> ConnectionContext:
    saved: dict = {}

    async def save(secrets: dict) -> None:
        saved.update(secrets)

    return ConnectionContext(
        connection_id="conn-1",
        tenant_id="tenant-1",
        provider="zoho_crm",
        config={"dc": "com"},
        secrets={"access_token": "tok", "refresh_token": "ref", "expires_at": 9e12},
        save_secrets=save,
    )
