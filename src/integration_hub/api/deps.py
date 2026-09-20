from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from integration_hub.config import get_settings


async def require_api_key(x_api_key: str = Header(default="")) -> None:
    configured = get_settings().service_api_key
    if not configured:
        return  # local/dev mode
    if not secrets.compare_digest(x_api_key, configured):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
