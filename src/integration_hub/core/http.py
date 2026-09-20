from __future__ import annotations

import asyncio
import random
from typing import Any, Awaitable, Callable, Mapping

import httpx

from integration_hub.core.errors import (
    AuthError,
    PermissionError_,
    RateLimitError,
    TransientError,
    ValidationFailed,
)
from integration_hub.core.ratelimit import TokenBucket
from integration_hub.logging_config import get_logger

log = get_logger(__name__)

AuthHook = Callable[[], Awaitable[Mapping[str, str]]]
"""Returns the headers required to authenticate a request (refreshing tokens if needed)."""


class ApiClient:
    """Shared HTTP transport: auth injection, rate limiting, retries and error normalisation.

    Every provider client composes this instead of talking to httpx directly, so retry,
    throttling and observability behave identically across integrations.
    """

    def __init__(
        self,
        *,
        provider: str,
        base_url: str,
        auth_hook: AuthHook | None = None,
        rate_per_second: float = 10.0,
        max_retries: int = 5,
        timeout: float = 30.0,
        default_headers: Mapping[str, str] | None = None,
        on_auth_failure: Callable[[], Awaitable[None]] | None = None,
    ):
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self._auth_hook = auth_hook
        self._bucket = TokenBucket(rate_per_second)
        self._max_retries = max_retries
        self._on_auth_failure = on_auth_failure
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Accept": "application/json", **(default_headers or {})},
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "ApiClient":
        return self

    async def __aexit__(self, *exc) -> bool:
        await self.aclose()
        return False

    def set_base_url(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        headers: Mapping[str, str] | None = None,
        expected_empty: bool = False,
    ) -> Any:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        attempt = 0
        refreshed = False

        while True:
            attempt += 1
            await self._bucket.acquire()
            req_headers: dict[str, str] = dict(headers or {})
            if self._auth_hook:
                req_headers.update(await self._auth_hook())

            try:
                resp = await self._client.request(
                    method, url, params=self._clean(params), json=json, headers=req_headers
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt > self._max_retries:
                    raise TransientError(f"{method} {url} failed: {exc}", provider=self.provider) from exc
                await self._sleep_backoff(attempt)
                continue

            if resp.status_code == 401 and not refreshed:
                # Token may have been revoked/expired early; force one refresh then retry once.
                refreshed = True
                if self._on_auth_failure:
                    await self._on_auth_failure()
                continue

            if resp.status_code < 400:
                if expected_empty or resp.status_code == 204 or not resp.content:
                    return None
                return resp.json()

            await self._raise_for_status(resp, attempt, method, url)

    async def _raise_for_status(self, resp: httpx.Response, attempt: int, method: str, url: str) -> None:
        body = self._safe_body(resp)
        ctx = {"provider": self.provider, "details": body}

        if resp.status_code == 401:
            raise AuthError(f"Unauthorized on {method} {url}", **ctx)
        if resp.status_code == 403:
            raise PermissionError_(f"Forbidden on {method} {url}", **ctx)
        if resp.status_code == 429:
            retry_after = self._retry_after(resp)
            if attempt > self._max_retries:
                raise RateLimitError(f"Rate limited on {method} {url}", retry_after=retry_after, **ctx)
            log.warning("rate limited, backing off", extra={"url": url, "retry_after": retry_after})
            await asyncio.sleep(retry_after if retry_after is not None else self._backoff(attempt))
            return
        if resp.status_code >= 500:
            if attempt > self._max_retries:
                raise TransientError(f"{resp.status_code} on {method} {url}", **ctx)
            await self._sleep_backoff(attempt)
            return
        raise ValidationFailed(f"{resp.status_code} on {method} {url}", **ctx)

    @staticmethod
    def _retry_after(resp: httpx.Response) -> float | None:
        raw = resp.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(60.0, (2 ** (attempt - 1))) * (0.5 + random.random() / 2)

    async def _sleep_backoff(self, attempt: int) -> None:
        await asyncio.sleep(self._backoff(attempt))

    @staticmethod
    def _safe_body(resp: httpx.Response) -> Any:
        try:
            return resp.json()
        except Exception:
            return resp.text[:2000]

    @staticmethod
    def _clean(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not params:
            return None
        return {k: v for k, v in params.items() if v is not None}
