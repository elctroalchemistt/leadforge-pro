from __future__ import annotations

import asyncio
import random
import time
from typing import Any

import httpx

from leadforge.config import settings


class RateLimiter:
    def __init__(self, rps: float) -> None:
        self.rps = max(0.1, rps)
        self._lock = asyncio.Lock()
        self._next_time = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.perf_counter()
            wait_for = self._next_time - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._next_time = time.perf_counter() + (1.0 / self.rps)


async def request_json(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    limiter: RateLimiter | None = None,
) -> dict[str, Any]:
    if limiter:
        await limiter.wait()

    last_exc: Exception | None = None
    for attempt in range(1, settings.HTTP_RETRIES + 1):
        try:
            r = await client.get(url, params=params, headers=headers, timeout=settings.HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            backoff = (settings.HTTP_BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
            await asyncio.sleep(backoff)

    raise RuntimeError(f"HTTP request failed after {settings.HTTP_RETRIES} retries: {last_exc}")