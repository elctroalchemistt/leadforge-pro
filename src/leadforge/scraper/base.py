from abc import ABC, abstractmethod
from leadforge.models import Lead

class ScraperProvider(ABC):
    @abstractmethod
    async def search(self, keyword: str, location: str, limit: int = 50) -> list[Lead]:
        raise NotImplementedError