from leadforge.config import settings
from leadforge.scraper.base import ScraperProvider
from leadforge.scraper.providers.mock import MockProvider
from leadforge.scraper.providers.places_api import PlacesApiProvider

def get_provider() -> ScraperProvider:
    p = settings.SCRAPER_PROVIDER.lower().strip()
    if p == "mock":
        return MockProvider()
    if p == "places_api":
        return PlacesApiProvider()
    raise ValueError(f"Unknown SCRAPER_PROVIDER: {settings.SCRAPER_PROVIDER}")