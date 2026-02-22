import asyncio
import time

class RateLimiter:
    def __init__(self, rps: float = 5.0) -> None:
        self.rps = max(0.1, float(rps))
        self._min_interval = 1.0 / self.rps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            sleep_for = self._min_interval - elapsed
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self._last = time.monotonic()