from __future__ import annotations
import re
import httpx
from leadforge.config import settings
from leadforge.models import Lead

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

async def extract_email_from_homepage(url: str) -> str | None:
    try:
        timeout = httpx.Timeout(settings.HTTP_TIMEOUT)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "LeadForgePro/0.2"})
            if r.status_code >= 400:
                return None
            text = r.text
            m = EMAIL_RE.search(text)
            return m.group(0) if m else None
    except Exception:
        return None

async def enrich_email(lead: Lead) -> Lead:
    if lead.email:
        return lead
    if lead.website and lead.website_valid:
        found = await extract_email_from_homepage(lead.website)
        lead.email = found
    return lead