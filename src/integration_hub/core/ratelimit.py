from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async token bucket used to stay under a provider's documented request rate."""

    def __init__(self, rate_per_second: float, burst: int | None = None):
        self.rate = max(rate_per_second, 0.001)
        self.capacity = burst if burst is not None else max(int(rate_per_second), 1)
        self._tokens = float(self.capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait_for = deficit / self.rate
            await asyncio.sleep(wait_for)


class ConcurrencyLimiter:
    def __init__(self, limit: int):
        self._sem = asyncio.Semaphore(limit)

    async def __aenter__(self):
        await self._sem.acquire()
        return self

    async def __aexit__(self, *exc):
        self._sem.release()
        return False
