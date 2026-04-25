"""
score_vibes.py — City Vibe Scoring Module
==========================================
Fetches signal from three sources and uses Claude to produce vibe scores:
  1. OpenStreetMap (Overpass API)  → hard quantitative POI counts
  2. Wikipedia REST API            → cultural / geographic context
  3. Wikivoyage REST API           → opinionated travel signal
                                     (safety, nightlife, budget, local tips)

Usage
-----
    # Score a single city, print results:
    python score_vibes.py --city "Medellín" --country "Colombia"

    # Score a CSV of cities, append to JSONL (resumable):
    python score_vibes.py --input filtered_cities.csv --output vibe_scores.jsonl

    # Skip OSM (faster, Wikipedia + Wikivoyage + Claude only):
    python score_vibes.py --input filtered_cities.csv --skip-osm

    # Convert an existing JSONL to flat CSV:
    python score_vibes.py --to-csv --output vibe_scores.jsonl

Environment
-----------
    ANTHROPIC_API_KEY   Required for Claude enrichment.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────

OSM_TIMEOUT = 30          # seconds per Overpass query
OSM_DELAY   = 1.5         # polite pause between city OSM batches
OSM_RADIUS  = 15_000      # metres around city centre

WIKI_DELAY      = 0.5
WIKIVOYAGE_DELAY = 0.5

CLAUDE_MODEL    = "claude-sonnet-4-20250514"
CLAUDE_TIMEOUT  = 45
CLAUDE_MAX_TOKS = 1_200

# ─── CONTROLLED VOCABULARY ─────────────────────────────────────────────────────

TAGS: list[str] = [
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

VIBE_SCORES: list[str] = [
    "adventure",   # hiking, extreme sports, outdoor activities
    "relaxation",  # spas, beaches, slow pace
    "culture",     # museums, history, art
    "nightlife",   # bars, clubs, live music
    "food",        # gastronomy quality & variety
    "nature",      # natural landscapes & wildlife
    "romance",     # couples, scenic, intimate
    "budget",      # affordability  (1 = very cheap, 0 = very expensive)
    "luxury",      # high-end options available
    "family",      # kid-friendly activities
    "exotic",      # how different/unusual vs typical western city
    "safety",      # perceived safety for tourists (1 = very safe)
]

# ─── OSM POI CATEGORIES ────────────────────────────────────────────────────────

OSM_QUERIES: dict[str, str] = {
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

# ─── SOURCE 1: OpenStreetMap ────────────────────────────────────────────────────

def fetch_osm_pois(lat: float, lng: float, radius: int = OSM_RADIUS) -> dict[str, int]:
    """
    Query Overpass API for POI counts around a city centre.
    Returns {poi_name: count}, with -1 as an error sentinel.
    """
    results: dict[str, int] = {}
    overpass_url = "https://overpass-api.de/api/interpreter"
    ua = "CityVibeScorer/1.0 (educational project)"

    for poi_name, osm_filter in OSM_QUERIES.items():
        query = (
            f"[out:json][timeout:{OSM_TIMEOUT}];\n"
            f"(\n  {osm_filter}(around:{radius},{lat},{lng});\n);\n"
            f"out count;"
        )
        try:
            resp = requests.post(
                overpass_url,
                data={"data": query},
                timeout=OSM_TIMEOUT + 5,
                headers={"User-Agent": ua},
            )
            if resp.status_code == 200:
                data = resp.json()
                count = (
                    data.get("elements", [{}])[0]
                        .get("tags", {})
                        .get("total", 0)
                )
                results[poi_name] = int(count)
            else:
                results[poi_name] = -1
        except Exception:
            results[poi_name] = -1

        time.sleep(0.3)  # small delay between POI types

    return results


# ─── SOURCE 2: Wikipedia ───────────────────────────────────────────────────────

def fetch_wikipedia_summary(city_name: str, country_name: str,
                            max_chars: int = 2_000) -> str:
    """
    Fetch the Wikipedia intro paragraph for a city.
    Returns an empty string on failure.
    """
    candidates = [f"{city_name}, {country_name}", city_name]
    ua = "CityVibeScorer/1.0 (educational project)"

    for q in candidates:
        url = (
            "https://en.wikipedia.org/api/rest_v1/page/summary/"
            + requests.utils.quote(q)
        )
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": ua})
            if resp.status_code == 200:
                extract = resp.json().get("extract", "")
                if len(extract) > 100:
                    return extract[:max_chars]
        except Exception:
            pass
        time.sleep(WIKI_DELAY)

    return ""


# ─── SOURCE 3: Wikivoyage ──────────────────────────────────────────────────────

_WIKIVOYAGE_SECTIONS_OF_INTEREST = {
    "Understand", "Get in", "Get around",
    "See", "Do", "Eat", "Drink", "Sleep",
    "Stay safe", "Stay healthy", "Cope", "Budget",
}

def _strip_html(text: str) -> str:
    """Very lightweight HTML tag stripper (no external deps)."""
    return re.sub(r"<[^>]+>", "", text)


def _extract_wikivoyage_sections(sections: list[dict]) -> str:
    """
    Pull the text from named sections of interest and return a
    combined plain-text blob (max ~3 000 chars).
    """
    parts: list[str] = []
    for section in sections:
        title = section.get("title", "")
        if title in _WIKIVOYAGE_SECTIONS_OF_INTEREST:
            body = _strip_html(section.get("content", "")).strip()
            if body:
                parts.append(f"=== {title} ===\n{body}")

    combined = "\n\n".join(parts)
    return combined[:3_000]


def fetch_wikivoyage(city_name: str, country_name: str) -> str:
    """
    Fetch travel-focused text from Wikivoyage via the MediaWiki REST API.

    Strategy:
      1. Search for the page title.
      2. Fetch full page sections.
      3. Extract only the travel-relevant sections.

    Returns a plain-text blob, or "" on failure.
    """
    ua = "CityVibeScorer/1.0 (educational project)"
    base = "https://en.wikivoyage.org/api/rest_v1"

    # ── Step 1: search for a matching page title ──────────────────────────────
    candidates = [f"{city_name}", f"{city_name}, {country_name}", city_name.split()[0]]
    page_title: str | None = None

    for q in candidates:
        try:
            search_url = f"https://en.wikivoyage.org/w/api.php"
            params = {
                "action": "query",
                "list": "search",
                "srsearch": q,
                "srlimit": 1,
                "format": "json",
            }
            resp = requests.get(search_url, params=params, timeout=10,
                                headers={"User-Agent": ua})
            if resp.status_code == 200:
                results = resp.json().get("query", {}).get("search", [])
                if results:
                    page_title = results[0]["title"]
                    break
        except Exception:
            pass
        time.sleep(WIKIVOYAGE_DELAY)

    if not page_title:
        return ""

    # ── Step 2: fetch page sections ───────────────────────────────────────────
    try:
        sections_url = f"{base}/page/mobile-sections/{requests.utils.quote(page_title)}"
        resp = requests.get(sections_url, timeout=12, headers={"User-Agent": ua})
        if resp.status_code != 200:
            return ""

        data = resp.json()

        # Lead section (intro)
        lead_text = _strip_html(
            data.get("lead", {}).get("sections", [{}])[0].get("text", "")
        ).strip()[:500]

        # Remaining sections
        remaining: list[dict] = data.get("remaining", {}).get("sections", [])
        body_text = _extract_wikivoyage_sections(remaining)

        combined = f"[Lead]\n{lead_text}\n\n{body_text}".strip()
        return combined[:3_500]

    except Exception:
        return ""


# ─── CLAUDE ENRICHMENT ─────────────────────────────────────────────────────────

def _build_prompt(city_name: str, country: str, continent: str,
                  wiki_text: str, wikivoyage_text: str, pois: dict) -> str:
    poi_lines = "\n".join(
        f"  - {k}: {v}" for k, v in pois.items() if v >= 0
    ) or "  (no OSM data)"

    wiki_block = wiki_text or "No Wikipedia data available."
    wv_block   = wikivoyage_text or "No Wikivoyage data available."

    return f"""You are a travel data analyst. Analyze this city and return a JSON object.

CITY: {city_name}, {country} ({continent})

━━━ SOURCE 1: WIKIPEDIA (cultural/geographic context) ━━━
{wiki_block}

━━━ SOURCE 2: WIKIVOYAGE (opinionated travel signal — safety, nightlife, budget) ━━━
{wv_block}

━━━ SOURCE 3: OPENSTREETMAP POI COUNTS (within 15 km radius) ━━━
{poi_lines}

Return ONLY valid JSON (no markdown, no explanation) with this exact structure:
{{
  "description": "2-3 sentence travel-focused description",
  "tags": ["tag1", "tag2"],
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
  }},
  "source_notes": "1-2 sentences on which sources drove the key scores"
}}

Rules:
- tags must ONLY come from this list: {json.dumps(TAGS)}
- Select 3-8 most relevant tags
- All vibe_scores must be floats between 0.0 and 1.0
- Prioritise Wikivoyage for safety, nightlife, and budget signals;
  Wikipedia for culture, history, nature; OSM for quantitative density
- budget: 1.0 = very affordable, 0.0 = very expensive
- safety: 1.0 = very safe, 0.0 = very unsafe
"""


def enrich_with_claude(city_name: str, country: str, continent: str,
                        wiki_text: str, wikivoyage_text: str,
                        pois: dict) -> dict:
    """
    Call Claude API to synthesise the three sources into tags + vibe scores.
    Falls back to a neutral empty structure on any error.
    """
    prompt = _build_prompt(city_name, country, continent,
                           wiki_text, wikivoyage_text, pois)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    if api_key:
        headers["x-api-key"] = api_key

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": CLAUDE_MAX_TOKS,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=CLAUDE_TIMEOUT,
        )
        if resp.status_code == 200:
            text = resp.json()["content"][0]["text"].strip()
            # Strip accidental markdown fences
            text = re.sub(r"^```(?:json)?\s*|```$", "", text,
                          flags=re.MULTILINE).strip()
            return json.loads(text)

        print(f"    Claude HTTP {resp.status_code}: {resp.text[:200]}")

    except Exception as exc:
        print(f"    Claude error: {exc}")

    # Fallback
    return {
        "description": "",
        "tags": [],
        "vibe_scores": {k: 0.5 for k in VIBE_SCORES},
        "source_notes": "enrichment failed",
    }


# ─── SINGLE-CITY ENTRY POINT ───────────────────────────────────────────────────

def score_city(city_name: str, country_name: str, continent: str = "Unknown",
               lat: float = 0.0, lng: float = 0.0,
               skip_osm: bool = False,
               verbose: bool = True) -> dict:
    """
    Score a single city. Returns the full enriched record dict.

    This is the main public API of this module — import and call directly:

        from score_vibes import score_city
        result = score_city("Kyoto", "Japan", continent="Asia",
                            lat=35.011, lng=135.768)
    """
    def log(msg: str) -> None:
        if verbose:
            print(msg)

    log(f"\n{'─'*60}")
    log(f"  {city_name}, {country_name}")
    log(f"{'─'*60}")

    # 1. OSM
    if skip_osm or (lat == 0.0 and lng == 0.0):
        pois: dict[str, int] = {k: -1 for k in OSM_QUERIES}
        log("  [OSM]       skipped")
    else:
        log(f"  [OSM]       fetching POIs within {OSM_RADIUS//1000} km…")
        pois = fetch_osm_pois(lat, lng)
        found = {k: v for k, v in pois.items() if v >= 0}
        if verbose and found:
            for name, cnt in sorted(found.items(), key=lambda x: -x[1]):
                log(f"              {name:<15} {cnt:>5}")
        time.sleep(OSM_DELAY)

    # 2. Wikipedia
    log("  [Wikipedia] fetching summary…")
    wiki = fetch_wikipedia_summary(city_name, country_name)
    log(f"              {len(wiki)} chars")

    # 3. Wikivoyage
    log("  [Wikivoyage] fetching travel sections…")
    wv = fetch_wikivoyage(city_name, country_name)
    log(f"              {len(wv)} chars")

    # 4. Claude
    log("  [Claude]    scoring…")
    enrichment = enrich_with_claude(city_name, country_name, continent,
                                    wiki, wv, pois)

    record = {
        "city_name":      city_name,
        "country_name":   country_name,
        "continent":      continent,
        "lat":            lat,
        "lng":            lng,
        "wiki_summary":   wiki,
        "wikivoyage_text": wv,
        "osm_pois":       pois,
        "description":    enrichment.get("description", ""),
        "tags":           enrichment.get("tags", []),
        "vibe_scores":    enrichment.get("vibe_scores", {}),
        "source_notes":   enrichment.get("source_notes", ""),
    }

    if verbose:
        log(f"  ✓ Tags:  {record['tags']}")
        vs = {k: round(v, 2) for k, v in record["vibe_scores"].items()}
        log(f"  ✓ Vibes: {vs}")
        if record["source_notes"]:
            log(f"  ✓ Notes: {record['source_notes']}")

    return record


# ─── BATCH PIPELINE ────────────────────────────────────────────────────────────

def _load_done(jsonl_path: str) -> set[str]:
    done: set[str] = set()
    if Path(jsonl_path).exists():
        with open(jsonl_path) as f:
            for line in f:
                try:
                    done.add(json.loads(line)["city_name"])
                except Exception:
                    pass
    return done


def run_pipeline(input_csv: str, output_jsonl: str,
                 limit: int | None = None,
                 skip_osm: bool = False) -> None:
    """
    Batch-score every city in a CSV (must have columns:
    city_name, country_name, continent, lat, lng).

    Resumable: already-processed cities are skipped.
    """
    df = pd.read_csv(input_csv)
    if limit:
        df = df.head(limit)

    done = _load_done(output_jsonl)
    remaining = len(df) - len(done)
    print(f"\nResuming: {len(done)} done, {remaining} to go.\n")

    with open(output_jsonl, "a") as out:
        for i, row in df.iterrows():
            city = row["city_name"]
            if city in done:
                continue

            record = score_city(
                city_name    = city,
                country_name = row["country_name"],
                continent    = row.get("continent", "Unknown"),
                lat          = float(row.get("lat", 0)),
                lng          = float(row.get("lng", 0)),
                skip_osm     = skip_osm,
                verbose      = True,
            )
            # Carry over any extra columns from the input CSV
            for col in ("city_ascii", "country_code", "population"):
                if col in row:
                    record[col] = row[col]

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()

    print(f"\nDone. Results → {output_jsonl}")


def jsonl_to_csv(jsonl_path: str, csv_path: str) -> None:
    """Flatten a JSONL file to a wide CSV for easy inspection."""
    records = []
    with open(jsonl_path) as f:
        for line in f:
            r = json.loads(line)
            flat = {
                "city_name":    r.get("city_name"),
                "country_name": r.get("country_name"),
                "continent":    r.get("continent"),
                "lat":          r.get("lat"),
                "lng":          r.get("lng"),
                "population":   r.get("population"),
                "description":  r.get("description"),
                "tags":         "|".join(r.get("tags", [])),
                "source_notes": r.get("source_notes", ""),
                **{f"vibe_{k}": v for k, v in r.get("vibe_scores", {}).items()},
                **{f"osm_{k}":  v for k, v in r.get("osm_pois",    {}).items()},
            }
            records.append(flat)

    pd.DataFrame(records).to_csv(csv_path, index=False)
    print(f"Flat CSV → {csv_path}  ({len(records)} cities)")


# ─── CLI ───────────────────────────────────────────────────────────────────────

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="City vibe scorer — Wikipedia + Wikivoyage + OSM + Claude",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--city",    metavar="NAME",
                      help="Score a single city (requires --country)")
    mode.add_argument("--input",   metavar="CSV",
                      help="Batch-score cities from a CSV file")
    mode.add_argument("--to-csv",  action="store_true",
                      help="Convert existing JSONL to flat CSV and exit")

    p.add_argument("--country",   metavar="NAME",
                   help="Country name (for --city mode)")
    p.add_argument("--continent", metavar="NAME", default="Unknown",
                   help="Continent (for --city mode, optional)")
    p.add_argument("--lat",  type=float, default=0.0,
                   help="Latitude (for --city mode + OSM)")
    p.add_argument("--lng",  type=float, default=0.0,
                   help="Longitude (for --city mode + OSM)")
    p.add_argument("--output", metavar="JSONL",
                   default="vibe_scores.jsonl",
                   help="Output JSONL path (default: vibe_scores.jsonl)")
    p.add_argument("--limit", type=int, default=None,
                   help="Process only first N cities (batch mode)")
    p.add_argument("--skip-osm", action="store_true",
                   help="Skip OSM fetching (faster; Wikipedia + Wikivoyage + Claude only)")
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()

    if args.to_csv:
        csv_path = args.output.replace(".jsonl", ".csv")
        jsonl_to_csv(args.output, csv_path)

    elif args.city:
        if not args.country:
            raise SystemExit("--city requires --country")
        score_city(
            city_name    = args.city,
            country_name = args.country,
            continent    = args.continent,
            lat          = args.lat,
            lng          = args.lng,
            skip_osm     = args.skip_osm,
            verbose      = True,
        )

    elif args.input:
        run_pipeline(
            input_csv    = args.input,
            output_jsonl = args.output,
            limit        = args.limit,
            skip_osm     = args.skip_osm,
        )
        if Path(args.output).exists():
            csv_path = args.output.replace(".jsonl", ".csv")
            jsonl_to_csv(args.output, csv_path)

    else:
        _build_arg_parser().print_help()


if __name__ == "__main__":
    main()