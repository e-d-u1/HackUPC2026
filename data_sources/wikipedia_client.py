"""
Wikipedia data source.

Uses the free Wikipedia REST API (no key required).
Endpoint docs: https://en.wikipedia.org/api/rest_v1/

Strategy:
  1. Search for "{city_name} {country}" to find the right article.
  2. Fetch the plain-text extract (up to ~400 words) to use as LLM input.
  3. Optionally grab the thumbnail for UI display.

Rate limits: Wikipedia asks for ≤200 req/s with a descriptive User-Agent.
We stay well below that with a configurable concurrency semaphore.
"""

import httpx
import asyncio
import re
from typing import Optional
from models.city import CityBase, WikipediaSummary


WIKI_REST = "https://en.wikipedia.org/api/rest_v1"
WIKI_API  = "https://en.wikipedia.org/w/api.php"

# Wikipedia's guidelines: identify your app in the User-Agent
USER_AGENT = "TravelIntelligencePlatform/0.1 (contact@example.com)"

# Max words we keep from the extract — enough for LLM context, not too many tokens
MAX_WORDS = 400


class WikipediaClient:
    """
    Async client for Wikipedia REST + MediaWiki API.

    Usage:
        async with WikipediaClient() as client:
            summary = await client.fetch_city_summary(city)
    """

    def __init__(self, concurrency: int = 5, timeout: float = 10.0):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_city_summary(self, city: CityBase) -> Optional[WikipediaSummary]:
        """
        Main entry point. Tries two strategies in order:
          1. Direct title lookup using the REST summary endpoint (fast).
          2. Full-text search via MediaWiki API if direct lookup fails.
        """
        async with self.semaphore:
            # Strategy 1: direct lookup  "{City}, {Country}"
            title = f"{city.name}, {city.country}"
            result = await self._fetch_by_title(title)
            if result:
                return result

            # Strategy 2: search and pick the top result
            title = await self._search_best_title(city.name, city.country_code)
            if title:
                result = await self._fetch_by_title(title)
                if result:
                    return result

            return None

    async def fetch_many(
        self,
        cities: list[CityBase],
        delay_between: float = 0.05,    # seconds between requests per worker
    ) -> dict[int, Optional[WikipediaSummary]]:
        """
        Fetch summaries for a list of cities concurrently.
        Returns {geonames_id: WikipediaSummary | None}.
        """
        async def _fetch_one(city: CityBase):
            await asyncio.sleep(delay_between)
            return city.geonames_id, await self.fetch_city_summary(city)

        results = await asyncio.gather(*[_fetch_one(c) for c in cities])
        return dict(results)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _fetch_by_title(self, title: str) -> Optional[WikipediaSummary]:
        """
        Use the REST summary endpoint — returns a rich JSON with extract,
        thumbnail, coordinates, etc.
        """
        # The REST endpoint uses URL path encoding
        encoded = title.replace(" ", "_")
        url = f"{WIKI_REST}/page/summary/{encoded}"

        try:
            resp = await self._client.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()

            extract = data.get("extract", "")
            if not extract or len(extract) < 50:
                return None

            return WikipediaSummary(
                title=data.get("title", title),
                extract=self._trim_extract(extract),
                page_url=data.get("content_urls", {})
                              .get("desktop", {})
                              .get("page", f"https://en.wikipedia.org/wiki/{encoded}"),
                thumbnail_url=(
                    data.get("thumbnail", {}).get("source")
                    if data.get("thumbnail") else None
                ),
            )
        except (httpx.HTTPStatusError, httpx.RequestError):
            return None

    async def _search_best_title(
        self, city_name: str, country_code: str
    ) -> Optional[str]:
        """
        Use the MediaWiki search API to find the most relevant article title.
        Falls back when the direct "{City}, {Country}" title doesn't exist.
        """
        try:
            resp = await self._client.get(WIKI_API, params={
                "action": "query",
                "list": "search",
                "srsearch": f"{city_name} city {country_code}",
                "srlimit": 3,
                "format": "json",
                "utf8": 1,
            })
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("query", {}).get("search", [])
            if not hits:
                return None

            # Pick the first hit whose title contains the city name
            for hit in hits:
                if city_name.lower() in hit["title"].lower():
                    return hit["title"]

            return hits[0]["title"] if hits else None
        except (httpx.HTTPStatusError, httpx.RequestError):
            return None

    @staticmethod
    def _trim_extract(text: str) -> str:
        """
        Trim to MAX_WORDS words, ending on a sentence boundary.
        Also strips parenthetical pronunciation guides like "(Lon-don /ˈlʌndən/)".
        """
        # Remove pronunciation guides
        text = re.sub(r"\s*\(.*?/.*?/.*?\)", "", text)
        # Remove bracketed citations [1], [2]
        text = re.sub(r"\[\d+\]", "", text)

        words = text.split()
        if len(words) <= MAX_WORDS:
            return text.strip()

        truncated = " ".join(words[:MAX_WORDS])
        # Walk back to find last sentence boundary
        last_period = max(
            truncated.rfind("."),
            truncated.rfind("!"),
            truncated.rfind("?"),
        )
        if last_period > MAX_WORDS * 3:   # at least 3 chars per word on average
            return truncated[:last_period + 1].strip()
        return truncated.strip() + "…"


# ------------------------------------------------------------------
# Quick smoke test  (run: python data_sources/wikipedia.py)
# ------------------------------------------------------------------

async def _demo():
    from models.city import CityBase

    sample_cities = [
        CityBase(geonames_id=2643743, name="London",    country="United Kingdom", country_code="GB", lat=51.5, lon=-0.1, population=8_961_989, timezone="Europe/London"),
        CityBase(geonames_id=3117735, name="Madrid",    country="Spain",          country_code="ES", lat=40.4, lon=-3.7, population=3_223_334, timezone="Europe/Madrid"),
        CityBase(geonames_id=1850147, name="Tokyo",     country="Japan",          country_code="JP", lat=35.7, lon=139.7, population=13_960_000, timezone="Asia/Tokyo"),
        CityBase(geonames_id=99999,   name="Faketown",  country="Nowhere",        country_code="XX", lat=0.0, lon=0.0, population=0, timezone="UTC"),
    ]

    async with WikipediaClient(concurrency=3) as client:
        results = await client.fetch_many(sample_cities)
        for city in sample_cities:
            summary = results[city.geonames_id]
            if summary:
                words = len(summary.extract.split())
                print(f"✓ {city.name}: {words} words — {summary.page_url}")
                print(f"  Preview: {summary.extract[:120]}…\n")
            else:
                print(f"✗ {city.name}: not found\n")

if __name__ == "__main__":
    asyncio.run(_demo())