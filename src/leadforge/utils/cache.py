from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from leadforge.config import settings


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


@dataclass
class Cache:
    dir: Path
    ttl_seconds: int = 60 * 60 * 24  # 24h

    def __post_init__(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.dir / f"{_hash_key(key)}.json"

    def get(self, key: str) -> Any | None:
        if not settings.CACHE_ENABLED:
            return None
        p = self._path(key)
        if not p.exists():
            return None
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            ts = payload.get("_ts")
            if not ts or (time.time() - ts) > self.ttl_seconds:
                return None
            return payload.get("data")
        except Exception:
            return None

    def set(self, key: str, data: Any) -> None:
        if not settings.CACHE_ENABLED:
            return
        p = self._path(key)
        payload = {"_ts": time.time(), "data": data}
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")