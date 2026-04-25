import asyncio
from typing import Optional

from models.city import CityBase
from data_sources.csvLoader import CSVCityLoader
from data_sources.wikipedia_client import WikipediaClient
from data_sources.osm import OverpassClient


class IngestionPipeline:
    """
    Runs only:
      Phase 1 → Wikipedia
      Phase 2 → OSM

    No disk writes. Returns raw dictionaries.
    """

    def __init__(
        self,
        skip_osm: bool = False,
        osm_radius_m: int = 15_000,
    ):
        self.skip_osm = skip_osm
        self.osm_radius_m = osm_radius_m

    # ------------------------------------------------------------------

    async def ingest_cities(
        self,
        cities: list[CityBase],
        wiki_concurrency: int = 5,
    ) -> dict:
        """
        Returns:
            {
                city_id: {
                    "city": CityBase,
                    "wikipedia": ...,
                    "osm_pois": ...
                }
            }
        """

        print(f"\n{'─'*60}")
        print(f"  Ingesting {len(cities)} cities")
        print(f"  Wikipedia concurrency : {wiki_concurrency}")
        print(f"  OSM enabled           : {not self.skip_osm}")
        print(f"{'─'*60}\n")

        # ── Phase 1: Wikipedia ─────────────────────────────────────────
        print("Phase 1/2  Fetching Wikipedia summaries…")
        async with WikipediaClient(concurrency=wiki_concurrency) as wiki:
            wiki_results = await wiki.fetch_many(cities)

        found = sum(1 for v in wiki_results.values() if v)
        print(f"  ✓ {found}/{len(cities)} summaries retrieved\n")

        # ── Phase 2: OSM ───────────────────────────────────────────────
        osm_results: dict[int, Optional[object]] = {c.geonames_id: None for c in cities}

        if not self.skip_osm:
            print("Phase 2/2  Fetching OSM POI counts…")
            async with OverpassClient(radius_m=self.osm_radius_m) as osm:
                osm_results = await osm.fetch_many(cities, delay_between_cities=2.0)

            found_osm = sum(1 for v in osm_results.values() if v)
            print(f"  ✓ {found_osm}/{len(cities)} OSM data retrieved\n")
        else:
            print("Phase 2/2  OSM skipped.\n")

        # ── Combine results (no saving) ─────────────────────────────────
        combined = {}

        for city in cities:
            combined[city.geonames_id] = {
                "city": city,
                "wikipedia": wiki_results.get(city.geonames_id),
                "osm_pois": osm_results.get(city.geonames_id),
            }

        return combined

    # ------------------------------------------------------------------

    async def ingest_from_csv(
        self,
        csv_path: str = "filtered_cities.csv",
        limit: int | None = None,
        continent: str | None = None,
        country_code: str | None = None,
        min_population: int = 0,
        balanced: bool = False,
        balanced_per_continent: int = 100,
        **kwargs,
    ) -> dict:

        loader = CSVCityLoader(csv_path)

        cities = loader.load(
            limit=limit,
            continent=continent,
            country_code=country_code,
            min_population=min_population,
            balanced=balanced,
            balanced_per_continent=balanced_per_continent,
        )

        print(f"  ✓ {len(cities)} cities loaded from {csv_path}\n")

        return await self.ingest_cities(cities, **kwargs)


# ------------------------------------------------------------------
# Demo
# ------------------------------------------------------------------

async def _demo():
    pipeline = IngestionPipeline(skip_osm=False, osm_radius_m=10_000)

    data = await pipeline.ingest_from_csv(
        csv_path="filtered_cities.csv",
        continent="Europe",
        limit=5,
        wiki_concurrency=3,
    )

    print("\nSample output:\n")

    for city_id, d in data.items():
        city = d["city"]
        wiki = d["wikipedia"]
        osm = d["osm_pois"]

        print(f"\n{'='*80}")
        print(f"CITY: {city.name}, {city.country}")
        print(f"{'='*80}")

        # Wikipedia summary
        if wiki:
            print(f"\n📖 WIKIPEDIA SUMMARY:")
            print(f"Title: {wiki.title}")
            print(f"URL: {wiki.page_url}")
            print(f"\nExtract:\n{wiki.extract}\n")
        else:
            print(f"\n📖 WIKIPEDIA: No data available\n")

        # OSM POIs
        if osm:
            print(f"\n📍 POI COUNTS (within {pipeline.osm_radius_m/1000:.0f}km):")
            poi_dict = osm.model_dump()
            # Group by category for better readability
            categories = {
                "🌳 Nature": ["beaches", "mountains", "parks", "forests"],
                "🎨 Culture": ["museums", "galleries", "theatres", "historic_sites"],
                "🍽️  Dining & Nightlife": ["bars", "nightclubs", "restaurants", "cafes"],
                "🏨 Accommodation": ["hotels", "hostels"],
                "✈️  Practical": ["airports", "universities"],
            }
            for category, keys in categories.items():
                print(f"\n  {category}")
                for key in keys:
                    count = poi_dict.get(key, 0)
                    print(f"    {key.replace('_', ' ').title():<20} {count:>4}")
        else:
            print(f"\n📍 POI COUNTS: No data available\n")
        

if __name__ == "__main__":
    asyncio.run(_demo())