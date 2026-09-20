from __future__ import annotations

from urllib.parse import urlencode

import httpx

from integration_hub.config import get_settings
from integration_hub.core.base import ConnectionContext
from integration_hub.core.errors import AuthError, ConfigurationError

# Zoho is region-partitioned: the account, API and accounts-server hosts must all match
# the data centre the customer's org lives in.
ACCOUNTS_HOSTS: dict[str, str] = {
    "com": "https://accounts.zoho.com",
    "eu": "https://accounts.zoho.eu",
    "in": "https://accounts.zoho.in",
    "com.au": "https://accounts.zoho.com.au",
    "jp": "https://accounts.zoho.jp",
    "ca": "https://accounts.zohocloud.ca",
    "com.cn": "https://accounts.zoho.com.cn",
    "sa": "https://accounts.zoho.sa",
}

API_HOSTS: dict[str, str] = {
    "com": "https://www.zohoapis.com",
    "eu": "https://www.zohoapis.eu",
    "in": "https://www.zohoapis.in",
    "com.au": "https://www.zohoapis.com.au",
    "jp": "https://www.zohoapis.jp",
    "ca": "https://www.zohoapis.ca",
    "com.cn": "https://www.zohoapis.com.cn",
    "sa": "https://www.zohoapis.sa",
}

DEFAULT_SCOPES = [
    "ZohoCRM.modules.ALL",
    "ZohoCRM.settings.modules.READ",
    "ZohoCRM.settings.fields.READ",
    "ZohoCRM.users.READ",
    "ZohoCRM.org.READ",
    "ZohoCRM.notifications.ALL",
    "ZohoCRM.coql.READ",
    "ZohoCRM.bulk.ALL",
]

# Refresh a little early so an in-flight request never races the expiry.
EXPIRY_SKEW_SECONDS = 120


def accounts_host(dc: str) -> str:
    try:
        return ACCOUNTS_HOSTS[dc]
    except KeyError as exc:
        raise ConfigurationError(f"Unsupported Zoho data centre '{dc}'") from exc


def api_host(dc: str) -> str:
    return API_HOSTS.get(dc, API_HOSTS["com"])


def build_authorize_url(*, dc: str, state: str, scopes: list[str] | None = None) -> str:
    s = get_settings()
    if not s.zoho_client_id:
        raise ConfigurationError("ZOHO_CLIENT_ID is not configured")
    params = {
        "scope": ",".join(scopes or DEFAULT_SCOPES),
        "client_id": s.zoho_client_id,
        "response_type": "code",
        # access_type=offline + prompt=consent is what makes Zoho return a refresh_token.
        "access_type": "offline",
        "prompt": "consent",
        "redirect_uri": s.zoho_redirect_uri,
        "state": state,
    }
    return f"{accounts_host(dc)}/oauth/v2/auth?{urlencode(params)}"


async def exchange_code(code: str, *, dc: str, accounts_server: str | None = None) -> dict:
    s = get_settings()
    host = accounts_server.rstrip("/") if accounts_server else accounts_host(dc)
    data = {
        "grant_type": "authorization_code",
        "client_id": s.zoho_client_id,
        "client_secret": s.zoho_client_secret,
        "redirect_uri": s.zoho_redirect_uri,
        "code": code,
    }
    return await _token_request(f"{host}/oauth/v2/token", data)


async def refresh_access_token(refresh_token: str, *, dc: str) -> dict:
    s = get_settings()
    data = {
        "grant_type": "refresh_token",
        "client_id": s.zoho_client_id,
        "client_secret": s.zoho_client_secret,
        "refresh_token": refresh_token,
    }
    return await _token_request(f"{accounts_host(dc)}/oauth/v2/token", data)


async def revoke_refresh_token(refresh_token: str, *, dc: str) -> None:
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(
            f"{accounts_host(dc)}/oauth/v2/token/revoke", data={"token": refresh_token}
        )


async def _token_request(url: str, data: dict) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, data=data)
    payload = resp.json() if resp.content else {}
    # Zoho returns HTTP 200 with an "error" key for invalid grants.
    if resp.status_code >= 400 or "error" in payload:
        raise AuthError(
            f"Zoho token request failed: {payload.get('error', resp.status_code)}",
            provider="zoho_crm",
            details=payload,
        )
    return payload


class ZohoTokenManager:
    """Owns access-token lifecycle for one connection and persists rotations."""

    def __init__(self, ctx: ConnectionContext):
        self.ctx = ctx
        self.dc: str = ctx.config.get("dc") or get_settings().zoho_default_dc

    @property
    def api_domain(self) -> str:
        return self.ctx.secrets.get("api_domain") or self.ctx.config.get("api_domain") or api_host(self.dc)

    def _expired(self) -> bool:
        import time

        expires_at = self.ctx.secrets.get("expires_at")
        return not self.ctx.secrets.get("access_token") or not expires_at or time.time() >= float(expires_at)

    async def headers(self) -> dict[str, str]:
        if self._expired():
            await self.refresh()
        return {"Authorization": f"Zoho-oauthtoken {self.ctx.secrets['access_token']}"}

    async def refresh(self) -> None:
        import time

        refresh_token = self.ctx.secrets.get("refresh_token")
        if not refresh_token:
            raise AuthError("Zoho connection has no refresh token; re-authorize", provider="zoho_crm")
        payload = await refresh_access_token(refresh_token, dc=self.dc)
        updates = {
            "access_token": payload["access_token"],
            "expires_at": time.time() + float(payload.get("expires_in", 3600)) - EXPIRY_SKEW_SECONDS,
        }
        if payload.get("api_domain"):
            updates["api_domain"] = payload["api_domain"]
        if payload.get("refresh_token"):
            updates["refresh_token"] = payload["refresh_token"]
        self.ctx.secrets.update(updates)
        await self.ctx.save_secrets(updates)

    async def invalidate(self) -> None:
        self.ctx.secrets.pop("expires_at", None)
        await self.refresh()
