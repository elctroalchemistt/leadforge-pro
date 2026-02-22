from __future__ import annotations

import asyncio

from leadforge.config import settings
from leadforge.models import Lead
from leadforge.enrichment.phone import enrich_phone
from leadforge.enrichment.website import enrich_website
from leadforge.enrichment.email import enrich_email
from leadforge.enrichment.dedupe import dedupe


async def enrich_all(leads: list[Lead], default_region: str = "US") -> list[Lead]:
    for l in leads:
        enrich_phone(l, default_region)
        enrich_website(l)

    sem = asyncio.Semaphore(settings.MAX_CONCURRENCY_EMAIL)

    async def _safe_email(l: Lead) -> Lead:
        async with sem:
            return await enrich_email(l)

    await asyncio.gather(*[_safe_email(l) for l in leads])
    return dedupe(leads)