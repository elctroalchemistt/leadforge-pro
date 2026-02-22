from __future__ import annotations

import asyncio
from urllib.parse import urlencode

import httpx

from leadforge.config import settings
from leadforge.models import Lead
from leadforge.scraper.base import ScraperProvider
from leadforge.utils.cache import Cache
from leadforge.utils.http import RateLimiter, request_json


TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


class PlacesApiProvider(ScraperProvider):
    def __init__(self) -> None:
        if not settings.GOOGLE_API_KEY:
            raise RuntimeError("GOOGLE_API_KEY is missing. Set it in .env or environment variables.")
        self.cache = Cache(dir=(__import__("pathlib").Path(settings.CACHE_DIR) / "places_api"), ttl_seconds=60 * 60 * 24)
        self.limiter = RateLimiter(settings.RPS)

    async def search(self, keyword: str, location: str, limit: int = 50) -> list[Lead]:
        query = f"{keyword} in {location}"
        leads: list[Lead] = []

        async with httpx.AsyncClient() as client:
            page_token: str | None = None

            while len(leads) < limit:
                params = {"query": query, "key": settings.GOOGLE_API_KEY}
                if page_token:
                    params["pagetoken"] = page_token

                cache_key = f"textsearch:{urlencode(params)}"
                data = self.cache.get(cache_key)
                if data is None:
                    data = await request_json(client, TEXTSEARCH_URL, params=params, limiter=self.limiter)
                    self.cache.set(cache_key, data)

                status = data.get("status")
                if status not in ("OK", "ZERO_RESULTS"):
                    # INVALID_REQUEST often means token not ready yet
                    if status == "INVALID_REQUEST" and page_token:
                        await asyncio.sleep(2.0)
                        continue
                    raise RuntimeError(f"Places TextSearch error: status={status} error={data.get('error_message')}")

                results = data.get("results", []) or []
                for r in results:
                    if len(leads) >= limit:
                        break
                    place_id = r.get("place_id")
                    name = r.get("name") or "Unknown"
                    rating = r.get("rating")
                    user_ratings_total = r.get("user_ratings_total")
                    types = r.get("types") or []
                    category = (types[0] if types else None)

                    lead = Lead(
                        name=name,
                        category=category,
                        rating=float(rating) if rating is not None else None,
                        review_count=int(user_ratings_total) if user_ratings_total is not None else None,
                        place_id=place_id,
                        source="places_api",
                        raw={"textsearch": r},
                    )
                    leads.append(lead)

                page_token = data.get("next_page_token")
                if not page_token:
                    break

                # Google says next_page_token may take a short time to become valid
                await asyncio.sleep(2.0)

            # Fetch details concurrently (controlled)
            sem = asyncio.Semaphore(settings.MAX_CONCURRENCY_DETAILS)

            async def _details(l: Lead) -> Lead:
                if not l.place_id:
                    return l
                async with sem:
                    return await self._fetch_details(client, l)

            leads = await asyncio.gather(*[_details(l) for l in leads[:limit]])

        return list(leads)

    async def _fetch_details(self, client: httpx.AsyncClient, lead: Lead) -> Lead:
        fields = "name,formatted_phone_number,website,adr_address,formatted_address"
        params = {"place_id": lead.place_id, "fields": fields, "key": settings.GOOGLE_API_KEY}

        cache_key = f"details:{urlencode(params)}"
        data = self.cache.get(cache_key)
        if data is None:
            data = await request_json(client, DETAILS_URL, params=params, limiter=self.limiter)
            self.cache.set(cache_key, data)

        status = data.get("status")
        if status != "OK":
            return lead

        result = data.get("result") or {}
        lead.phone = result.get("formatted_phone_number") or lead.phone
        lead.website = result.get("website") or lead.website

        # formatted_address example: "123 Main St, Chicago, IL 60601, USA"
        addr = result.get("formatted_address")
        if addr and (lead.city is None or lead.state is None):
            from leadforge.utils.address import parse_city_state_from_address
            city, state = parse_city_state_from_address(addr)
            lead.city = lead.city or city
            lead.state = lead.state or state

        # keep raw
        lead.raw = lead.raw or {}
        lead.raw["details"] = result
        return lead