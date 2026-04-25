"""
wikidata_client.py
------------------
Fetches a seed list of global cities from the Wikidata SPARQL endpoint.

Returns structured records:
  - wikidata_id, name, country, country_code, lat, lon,
    population, wiki_title, continent

Usage:
    from src.wikidata_client import WikidataClient
    client = WikidataClient()
    cities = client.fetch_cities(min_population=200_000, limit=1000)
"""

import time
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SPARQL query — fetches cities with coordinates, country, population, and
# their English Wikipedia sitelink. Filters to instance-of "city" or
# "big city". Results ordered by population descending so we get the most
# relevant cities first when using a limit.
# ---------------------------------------------------------------------------
CITIES_SPARQL = """
SELECT DISTINCT
  ?city
  ?cityLabel
  ?countryLabel
  ?country_code
  ?coords
  ?population
  ?continentLabel
WHERE {{
  ?city wdt:P31/wdt:P279* wd:Q515 .
  ?city wdt:P17 ?country .
  ?city wdt:P625 ?coords .

  OPTIONAL {{ ?city wdt:P1082 ?population . }}
  OPTIONAL {{ ?country wdt:P297 ?country_code . }}
  OPTIONAL {{ ?city wdt:P30 ?continent . }}

  FILTER(?population > {min_population})

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY DESC(?population)
LIMIT {limit}
"""
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

HEADERS = {
    "User-Agent": "CityIntelligencePipeline/1.0 (travel-platform-mvp; contact@example.com)",
    "Accept": "application/sparql-results+json",
}


class WikidataClient:
    def __init__(
        self,
        endpoint: str = WIKIDATA_ENDPOINT,
        request_timeout: int = 120,
        retry_attempts: int = 3,
        retry_backoff: float = 5.0,
    ):
        self.endpoint = endpoint
        self.timeout = request_timeout
        self.retry_attempts = retry_attempts
        self.retry_backoff = retry_backoff
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_cities(
      self,
      min_population: int = 200_000,
      limit: int = 1000,
      continent_filter: Optional[list[str]] = None,
   ) -> list[dict]:

      query = CITIES_SPARQL.format(
         min_population=min_population,
         limit=limit,
      )

      if continent_filter:
         values_block = "VALUES ?continent { " + " ".join(
               f"wd:{q}" for q in continent_filter
         ) + " }"

         query = query.replace(
               "WHERE {{",
               f"WHERE {{\n  {values_block}",
         )

      # 🧪 DEBUG 1: QUERY FINAL
      print("\n========== SPARQL QUERY ==========")
      print(query)
      print("==================================\n")

      raw = self._execute_query(query)

      # 🧪 DEBUG 2: RAW RESPONSE STRUCTURE
      print("\n========== RAW RESPONSE KEYS ==========")
      print(raw.keys())
      print("======================================\n")

      if "results" in raw:
         print("\n========== RESULTS KEYS ==========")
         print(raw["results"].keys())
         print("=================================\n")

         bindings = raw["results"].get("bindings", [])

         # 🧪 DEBUG 3: NUMBER OF ROWS FROM API
         print(f"\n🔥 RAW BINDINGS COUNT: {len(bindings)}\n")

         # 🧪 DEBUG 4: SHOW FIRST ROW RAW
         if bindings:
               print("\n========== FIRST RAW ROW ==========")
               print(bindings[0])
               print("==================================\n")

      cities = self._parse_results(raw)

      # 🧪 DEBUG 5: FINAL PARSED OUTPUT
      print(f"\n🚀 FINAL PARSED CITIES: {len(cities)}\n")

      for c in cities[:3]:
         print("CITY:", c)

      logger.info("Wikidata returned %d cities", len(cities))
      return cities

    def fetch_single_city(self, wikidata_id: str) -> Optional[dict]:
        """Fetch a single city by its Wikidata QID (e.g. 'Q90' for Paris)."""
        query = f"""
        SELECT ?cityLabel ?countryLabel ?country_code ?lat ?lon ?population ?wiki_title ?continentLabel
        WHERE {{
          BIND(wd:{wikidata_id} AS ?city)
          ?city wdt:P17 ?country .
          ?city wdt:P625 ?coords .
          OPTIONAL {{ ?city wdt:P1082 ?population . }}
          OPTIONAL {{ ?country wdt:P297 ?country_code . }}
          OPTIONAL {{ ?city wdt:P30 ?continent . }}
          ?article schema:about ?city ;
                   schema:isPartOf <https://en.wikipedia.org/> ;
                   schema:name ?wiki_title .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 1
        """
        logger.debug("FINAL SPARQL QUERY:\n%s", query)
        raw = self._execute_query(query)
        results = self._parse_results(raw, wikidata_id=wikidata_id)
        return results[0] if results else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_query(self, query: str) -> dict:
        """Run a SPARQL query with retry + exponential backoff."""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                logger.debug("SPARQL query attempt %d/%d", attempt, self.retry_attempts)
                response = self.session.get(
                    self.endpoint,
                    params={"query": query, "format": "json"},
                    timeout=self.timeout,
                )
                print("\n========== HTTP STATUS ==========")
                print(response.status_code)
                print("================================\n")

                print("\n========== RESPONSE TEXT (first 300 chars) ==========")
                print(response.text[:300])
                print("====================================================\n")
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                # 429 = rate limited — always back off
                if e.response is not None and e.response.status_code == 429:
                    wait = self.retry_backoff * (2 ** attempt)
                    logger.warning("Rate limited by Wikidata. Waiting %.0fs…", wait)
                    time.sleep(wait)
                elif attempt == self.retry_attempts:
                    raise
                else:
                    logger.warning("HTTP error on attempt %d: %s", attempt, e)
                    time.sleep(self.retry_backoff)

            except requests.exceptions.Timeout:
                logger.warning("Timeout on attempt %d/%d", attempt, self.retry_attempts)
                if attempt == self.retry_attempts:
                    raise
                time.sleep(self.retry_backoff)

            except requests.exceptions.RequestException as e:
                logger.error("Request failed: %s", e)
                raise

        return {}

    def _parse_results(self, raw: dict, wikidata_id: Optional[str] = None) -> list[dict]:
        """Parse SPARQL JSON results into clean city dicts."""
        if not raw or "results" not in raw:
            return []

        bindings = raw["results"].get("bindings", [])
        cities = []

        for row in bindings:
            def val(key: str) -> Optional[str]:
                return row[key]["value"] if key in row else None

            # Extract QID from full URI, e.g.
            # "http://www.wikidata.org/entity/Q90" → "Q90"
            city_uri = val("city") or ""
            qid = wikidata_id or (city_uri.split("/")[-1] if city_uri else None)

            # Parse coordinates — Wikidata returns "Point(lon lat)"
            lat, lon = self._parse_coords(val("coords"))

            city = {
                "wikidata_id":   qid,
                "name":          val("cityLabel"),
                "country":       val("countryLabel"),
                "country_code":  (val("country_code") or "").upper() or None,
                "lat":           lat,
                "lon":           lon,
                "population":    self._parse_int(val("population")),
                "continent":     val("continentLabel"),
            }

            # Skip rows missing critical fields
            if not city["name"]:
                continue
            if city["lat"] is None or city["lon"] is None:
                continue

            cities.append(city)

        # Deduplicate by wikidata_id (SPARQL can return dupes if a city has
        # multiple continent/population triples)
        seen = set()
        unique = []
        for c in cities:
            if c["wikidata_id"] not in seen:
                seen.add(c["wikidata_id"])
                unique.append(c)

        return unique

    @staticmethod
    def _parse_coords(coords_str: Optional[str]) -> tuple[Optional[float], Optional[float]]:
        """Parse 'Point(lon lat)' string into (lat, lon) floats."""
        if not coords_str:
            return None, None
        try:
            # Format: "Point(2.3488 48.8534)"  → lon first, lat second
            inner = coords_str.replace("Point(", "").replace(")", "").strip()
            parts = inner.split()
            return float(parts[1]), float(parts[0])  # lat, lon
        except (ValueError, IndexError):
            return None, None

    @staticmethod
    def _parse_int(val: Optional[str]) -> Optional[int]:
        try:
            return int(float(val)) if val else None
        except (ValueError, TypeError):
            return None