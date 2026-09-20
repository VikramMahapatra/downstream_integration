from __future__ import annotations

from typing import Any

from integration_hub.config import get_settings
from integration_hub.core.base import ConnectionContext
from integration_hub.core.http import ApiClient
from integration_hub.providers.zoho_crm.auth import ZohoTokenManager

# Zoho meters API "credits" per org per day; 10 rps keeps burst usage well inside
# the concurrency ceiling while the token bucket smooths spikes.
DEFAULT_RATE_PER_SECOND = 10.0
MAX_PER_PAGE = 200
MAX_WRITE_BATCH = 100


class ZohoCrmClient:
    def __init__(self, ctx: ConnectionContext):
        settings = get_settings()
        self.tokens = ZohoTokenManager(ctx)
        self.version = ctx.config.get("api_version") or settings.zoho_api_version
        self._api = ApiClient(
            provider="zoho_crm",
            base_url=f"{self.tokens.api_domain}/crm/{self.version}",
            auth_hook=self.tokens.headers,
            rate_per_second=float(ctx.config.get("rate_per_second") or DEFAULT_RATE_PER_SECOND),
            max_retries=settings.http_max_retries,
            timeout=settings.http_timeout_seconds,
            on_auth_failure=self.tokens.invalidate,
        )

    async def aclose(self) -> None:
        await self._api.aclose()

    def _sync_base_url(self) -> None:
        self._api.set_base_url(f"{self.tokens.api_domain}/crm/{self.version}")

    # --- metadata ---
    async def get_org(self) -> dict:
        self._sync_base_url()
        return await self._api.request("GET", "/org")

    async def list_modules(self) -> dict:
        return await self._api.request("GET", "/settings/modules")

    async def list_fields(self, module: str) -> dict:
        return await self._api.request("GET", "/settings/fields", params={"module": module})

    # --- reads ---
    async def get_records(
        self,
        module: str,
        *,
        per_page: int = MAX_PER_PAGE,
        page_token: str | None = None,
        fields: list[str] | None = None,
        modified_since: str | None = None,
        sort_by: str = "Modified_Time",
        sort_order: str = "asc",
    ) -> dict:
        self._sync_base_url()
        headers = {"If-Modified-Since": modified_since} if modified_since else None
        params: dict[str, Any] = {
            "per_page": min(per_page, MAX_PER_PAGE),
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        if fields:
            params["fields"] = ",".join(fields)
        if page_token:
            params["page_token"] = page_token
        return await self._api.request("GET", f"/{module}", params=params, headers=headers) or {}

    async def get_records_by_ids(self, module: str, ids: list[str], fields: list[str] | None = None) -> dict:
        """Used by the webhook path to hydrate a change notification."""
        self._sync_base_url()
        params: dict[str, Any] = {"ids": ",".join(ids)}
        if fields:
            params["fields"] = ",".join(fields)
        return await self._api.request("GET", f"/{module}", params=params) or {}

    async def get_deleted(self, module: str, *, page_token: str | None = None) -> dict:
        self._sync_base_url()
        params = {"type": "all", "per_page": MAX_PER_PAGE}
        if page_token:
            params["page_token"] = page_token
        return await self._api.request("GET", f"/{module}/deleted", params=params) or {}

    async def search(self, module: str, criteria: str, *, per_page: int = MAX_PER_PAGE) -> dict:
        self._sync_base_url()
        return (
            await self._api.request(
                "GET", f"/{module}/search", params={"criteria": criteria, "per_page": per_page}
            )
            or {}
        )

    async def coql(self, query: str) -> dict:
        self._sync_base_url()
        return await self._api.request("POST", "/coql", json={"select_query": query}) or {}

    # --- writes ---
    async def insert(self, module: str, records: list[dict], *, trigger: list[str] | None = None) -> dict:
        return await self._mutate("POST", f"/{module}", records, trigger)

    async def update(self, module: str, records: list[dict], *, trigger: list[str] | None = None) -> dict:
        return await self._mutate("PUT", f"/{module}", records, trigger)

    async def upsert(
        self,
        module: str,
        records: list[dict],
        *,
        duplicate_check_fields: list[str] | None = None,
        trigger: list[str] | None = None,
    ) -> dict:
        self._sync_base_url()
        body: dict[str, Any] = {"data": records[:MAX_WRITE_BATCH]}
        if duplicate_check_fields:
            body["duplicate_check_fields"] = duplicate_check_fields
        # An empty trigger list suppresses workflows/assignment rules during bulk loads.
        body["trigger"] = trigger if trigger is not None else []
        return await self._api.request("POST", f"/{module}/upsert", json=body) or {}

    async def delete(self, module: str, ids: list[str], *, wf_trigger: bool = False) -> dict:
        self._sync_base_url()
        return (
            await self._api.request(
                "DELETE",
                f"/{module}",
                params={"ids": ",".join(ids[:MAX_WRITE_BATCH]), "wf_trigger": str(wf_trigger).lower()},
            )
            or {}
        )

    async def _mutate(self, method: str, path: str, records: list[dict], trigger: list[str] | None) -> dict:
        self._sync_base_url()
        body = {"data": records[:MAX_WRITE_BATCH], "trigger": trigger if trigger is not None else []}
        return await self._api.request(method, path, json=body) or {}

    # --- notifications (webhooks) ---
    async def watch(self, payload: dict) -> dict:
        self._sync_base_url()
        return await self._api.request("POST", "/actions/watch", json=payload) or {}

    async def unwatch(self, channel_ids: list[str]) -> dict:
        self._sync_base_url()
        return (
            await self._api.request(
                "DELETE", "/actions/watch", params={"channel_ids": ",".join(channel_ids)}
            )
            or {}
        )
