"""
Phase 2: City Enrichment Pipeline
- Fetches POI counts from OpenStreetMap (Overpass API)
- Fetches Wikipedia summaries
- Uses Claude API to extract tags + vibe scores
- Outputs enriched_cities.jsonl (resumable) + enriched_cities.csv
"""

import pandas as pd
import requests
import json
import time
import os
import re
from pathlib import Path

# ─── CONFIG ────────────────────────────────────────────────────────────────────

INPUT_CSV = "filtered_cities.csv"
OUTPUT_JSONL = "enriched_cities.jsonl"
OUTPUT_CSV = "enriched_cities.csv"

# Overpass timeout (seconds per city)
OSM_TIMEOUT = 30
# Seconds between OSM requests (be polite)
OSM_DELAY = 1.5
# Seconds between Wikipedia requests
WIKI_DELAY = 0.5
# Radius around city center for POI search (meters)
OSM_RADIUS = 15000  # 15km

# ─── CONTROLLED VOCABULARY ─────────────────────────────────────────────────────

TAGS = [
    # Nature & geography
    "beach", "mountains", "desert", "jungle", "islands", "lakes", "rivers",
    # Urban character
    "historic", "modern", "architecture", "street_art", "markets",
    # Experiences
    "nightlife", "food_scene", "shopping", "festivals", "sports",
    # Culture
    "museums", "art", "music", "religion", "unesco",
    # Vibe keywords
    "romantic", "family_friendly", "solo_travel", "backpacker",
    # Practical
    "budget", "luxury", "safety", "walkable",
]

VIBE_SCORES = [
    "adventure",   # hiking, extreme sports, outdoor activities
    "relaxation",  # spas, beaches, slow pace
    "culture",     # museums, history, art
    "nightlife",   # bars, clubs, live music
    "food",        # gastronomy quality & variety
    "nature",      # natural landscapes & wildlife
    "romance",     # couples, scenic, intimate
    "budget",      # affordability (1 = very cheap, 0 = very expensive)
    "luxury",      # high-end options available
    "family",      # kid-friendly activities
    "exotic",      # how different/unusual vs typical western city
    "safety",      # perceived safety for tourists
]

# ─── OSM POI CATEGORIES ────────────────────────────────────────────────────────

OSM_QUERIES = {
    "beaches":     'way["natural"="beach"]',
    "museums":     'node["tourism"="museum"]',
    "bars":        'node["amenity"="bar"]',
    "nightclubs":  'node["amenity"="nightclub"]',
    "restaurants": 'node["amenity"="restaurant"]',
    "hotels":      'node["tourism"="hotel"]',
    "parks":       'node["leisure"="park"]',
    "historic":    'node["historic"]',
    "mountains":   'node["natural"="peak"]',
    "viewpoints":  'node["tourism"="viewpoint"]',
    "attractions": 'node["tourism"="attraction"]',
    "shopping":    'node["shop"="mall"]',
}


def fetch_osm_pois(lat: float, lng: float, radius: int = OSM_RADIUS) -> dict:
    """Query Overpass API for POI counts around a city center."""
    results = {}
    overpass_url = "https://overpass-api.de/api/interpreter"

    for poi_name, osm_filter in OSM_QUERIES.items():
        query = f"""
        [out:json][timeout:{OSM_TIMEOUT}];
        (
          {osm_filter}(around:{radius},{lat},{lng});
        );
        out count;
        """
        try:
            resp = requests.post(
                overpass_url,
                data={"data": query},
                timeout=OSM_TIMEOUT + 5,
                headers={"User-Agent": "TravelIntelligencePlatform/1.0 (educational project)"}
            )
            if resp.status_code == 200:
                data = resp.json()
                count = data.get("elements", [{}])[0].get("tags", {}).get("total", 0)
                results[poi_name] = int(count)
            else:
                results[poi_name] = -1  # error sentinel
        except Exception as e:
            results[poi_name] = -1
        time.sleep(0.3)  # small delay between POI types

    return results


def fetch_wikipedia_summary(city_name: str, country_name: str) -> str:
    """Fetch Wikipedia intro paragraph for a city."""
    # Try city + country first, then just city
    queries = [
        f"{city_name}, {country_name}",
        city_name,
    ]
    for q in queries:
        try:
            url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + requests.utils.quote(q)
            resp = requests.get(url, timeout=10,
                                headers={"User-Agent": "TravelIntelligencePlatform/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                # Make sure it's actually about the city
                extract = data.get("extract", "")
                if len(extract) > 100:
                    return extract[:2000]  # cap at 2000 chars
        except Exception:
            pass
        time.sleep(WIKI_DELAY)
    return ""


def enrich_with_claude(city_name: str, country: str, continent: str,
                        wiki_text: str, pois: dict) -> dict:
    """Call Claude API to extract tags and vibe scores."""

    poi_summary = "\n".join(
        f"  - {k}: {v} (within 15km)" for k, v in pois.items() if v >= 0
    )

    prompt = f"""You are a travel data analyst. Analyze this city and return a JSON object.

CITY: {city_name}, {country} ({continent})

WIKIPEDIA SUMMARY:
{wiki_text if wiki_text else "No Wikipedia data available."}

OPENSTREETMAP POI COUNTS (within 15km radius):
{poi_summary if poi_summary else "No OSM data available."}

Return ONLY valid JSON (no markdown, no explanation) with this exact structure:
{{
  "description": "2-3 sentence travel-focused description",
  "tags": ["tag1", "tag2", ...],
  "vibe_scores": {{
    "adventure": 0.0,
    "relaxation": 0.0,
    "culture": 0.0,
    "nightlife": 0.0,
    "food": 0.0,
    "nature": 0.0,
    "romance": 0.0,
    "budget": 0.0,
    "luxury": 0.0,
    "family": 0.0,
    "exotic": 0.0,
    "safety": 0.0
  }}
}}

Rules:
- tags must ONLY come from this list: {json.dumps(TAGS)}
- Select 3-8 most relevant tags
- All vibe_scores must be floats between 0.0 and 1.0
- Be data-driven: use the Wikipedia text and POI counts as primary evidence
- budget score: 1.0 = very affordable, 0.0 = very expensive
- safety score: 1.0 = very safe, 0.0 = very unsafe
"""

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            text = data["content"][0]["text"].strip()
            # Strip markdown fences if any
            text = re.sub(r"^```json\s*|```$", "", text, flags=re.MULTILINE).strip()
            return json.loads(text)
    except Exception as e:
        print(f"    Claude error: {e}")

    # Fallback empty structure
    return {
        "description": "",
        "tags": [],
        "vibe_scores": {k: 0.5 for k in VIBE_SCORES}
    }


def load_already_processed(jsonl_path: str) -> set:
    """Return set of city_names already in the output file (for resuming)."""
    done = set()
    if Path(jsonl_path).exists():
        with open(jsonl_path) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    done.add(obj["city_name"])
                except Exception:
                    pass
    return done


def run_pipeline(input_csv: str, output_jsonl: str, limit: int = None,
                 skip_osm: bool = False):
    """
    Main pipeline loop.

    Args:
        input_csv:    Path to filtered_cities.csv
        output_jsonl: Where to append enriched records
        limit:        Process only first N cities (None = all)
        skip_osm:     Skip OSM fetching (faster testing, uses Wikipedia + Claude only)
    """
    df = pd.read_csv(input_csv)
    if limit:
        df = df.head(limit)

    already_done = load_already_processed(output_jsonl)
    print(f"Resuming: {len(already_done)} cities already enriched, "
          f"{len(df) - len(already_done)} remaining.\n")

    with open(output_jsonl, "a") as out:
        for i, row in df.iterrows():
            city = row["city_name"]
            country = row["country_name"]
            continent = row["continent"]
            lat = row["lat"]
            lng = row["lng"]

            if city in already_done:
                continue

            print(f"[{i+1}/{len(df)}] {city}, {country}...")

            # 1. OSM POIs
            if skip_osm:
                pois = {k: -1 for k in OSM_QUERIES}
            else:
                print("  → Fetching OSM POIs...")
                pois = fetch_osm_pois(lat, lng)
                # Print POI counts
                poi_found = {k: v for k, v in pois.items() if v >= 0}
                if poi_found:
                    print("     POI Counts (within 15km):")
                    for poi_name, count in sorted(poi_found.items(), key=lambda x: x[1], reverse=True):
                        print(f"       • {poi_name:<15} {count:>4}")
                time.sleep(OSM_DELAY)

            # 2. Wikipedia
            print("  → Fetching Wikipedia...")
            wiki = fetch_wikipedia_summary(city, country)
            time.sleep(WIKI_DELAY)

            # 3. Claude enrichment
            print("  → Calling Claude...")
            enrichment = enrich_with_claude(city, country, continent, wiki, pois)

            # 4. Assemble record
            record = {
                "city_name": city,
                "city_ascii": row.get("city_ascii", city),
                "country_name": country,
                "country_code": row["country_code"],
                "continent": continent,
                "lat": lat,
                "lng": lng,
                "population": row["population"],
                "wiki_summary": wiki,
                "osm_pois": pois,
                "description": enrichment.get("description", ""),
                "tags": enrichment.get("tags", []),
                "vibe_scores": enrichment.get("vibe_scores", {}),
            }

            out.write(json.dumps(record) + "\n")
            out.flush()
            print(f"  ✓ Tags: {record['tags']}")
            print(f"  ✓ Vibes: { {k: round(v,2) for k,v in record['vibe_scores'].items()} }\n")

    print(f"\nDone! Results saved to {output_jsonl}")


def jsonl_to_csv(jsonl_path: str, csv_path: str):
    """Flatten JSONL to a CSV for easy inspection."""
    records = []
    with open(jsonl_path) as f:
        for line in f:
            r = json.loads(line)
            flat = {
                "city_name": r["city_name"],
                "country_name": r["country_name"],
                "country_code": r["country_code"],
                "continent": r["continent"],
                "lat": r["lat"],
                "lng": r["lng"],
                "population": r["population"],
                "description": r["description"],
                "tags": "|".join(r.get("tags", [])),
                **{f"vibe_{k}": v for k, v in r.get("vibe_scores", {}).items()},
                **{f"osm_{k}": v for k, v in r.get("osm_pois", {}).items()},
            }
            records.append(flat)

    pd.DataFrame(records).to_csv(csv_path, index=False)
    print(f"Flat CSV saved to {csv_path} ({len(records)} cities)")


# ─── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="City enrichment pipeline")
    parser.add_argument("--input", default=INPUT_CSV)
    parser.add_argument("--output", default=OUTPUT_JSONL)
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N cities")
    parser.add_argument("--skip-osm", action="store_true",
                        help="Skip OSM (Wikipedia + Claude only, much faster)")
    parser.add_argument("--to-csv", action="store_true",
                        help="Convert existing JSONL to flat CSV and exit")
    args = parser.parse_args()

    if args.to_csv:
        jsonl_to_csv(args.output, OUTPUT_CSV)
    else:
        run_pipeline(
            input_csv=args.input,
            output_jsonl=args.output,
            limit=args.limit,
            skip_osm=args.skip_osm,
        )
        # Auto-convert to CSV when done
        if Path(args.output).exists():
            jsonl_to_csv(args.output, OUTPUT_CSV)