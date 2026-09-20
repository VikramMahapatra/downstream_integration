from __future__ import annotations

import json
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from integration_hub.config import get_settings
from integration_hub.core.errors import ConfigurationError


@lru_cache
def _fernet() -> Fernet:
    key = get_settings().secret_encryption_key
    if not key:
        raise ConfigurationError(
            "SECRET_ENCRYPTION_KEY is not set. Generate one with "
            '`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`'
        )
    return Fernet(key.encode())


def encrypt_secrets(payload: dict) -> str:
    return _fernet().encrypt(json.dumps(payload, separators=(",", ":")).encode()).decode()


def decrypt_secrets(blob: str | None) -> dict:
    if not blob:
        return {}
    try:
        return json.loads(_fernet().decrypt(blob.encode()).decode())
    except InvalidToken as exc:  # rotated/incorrect key
        raise ConfigurationError("Stored credentials cannot be decrypted with the current key") from exc


def mask(value: str | None, keep: int = 4) -> str:
    if not value:
        return ""
    return "*" * max(len(value) - keep, 0) + value[-keep:]
