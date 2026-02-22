from __future__ import annotations

import json
import os
from pathlib import Path

from leadforge.models import Lead
from leadforge.scraper.base import ScraperProvider


class MockProvider(ScraperProvider):
    def __init__(self, sample_path: str | None = None) -> None:
        # Priority:
        # 1) explicit argument
        # 2) env variable
        # 3) project_root/examples/sample_leads.json

        env_path = os.getenv("LEADFORGE_SAMPLE_PATH")

        if sample_path:
            self.sample_path = Path(sample_path)
            return

        if env_path:
            self.sample_path = Path(env_path)
            return

        # Correct project root resolution
        # This file: .../leadforge-pro/src/leadforge/scraper/providers/mock.py
        # parents:
        # 0 providers
        # 1 scraper
        # 2 leadforge
        # 3 src
        # 4 leadforge-pro   <-- this is what we want
        project_root = Path(__file__).resolve().parents[4]

        self.sample_path = project_root / "examples" / "sample_leads.json"

    async def search(self, keyword: str, location: str, limit: int = 50) -> list[Lead]:
        if not self.sample_path.exists():
            raise FileNotFoundError(
                f"Mock sample file not found at: {self.sample_path}"
            )

        data = json.loads(self.sample_path.read_text(encoding="utf-8"))
        leads = [Lead(**x) for x in data][:limit]

        for l in leads:
            l.source = "mock"

        return leads