import pandas as pd
import requests
import time
import csv
import math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ─── CONFIG ─────────────────────────────────────────────

INPUT_CSV = "filtered_cities.csv"
OUTPUT_CSV = "cities_pois.csv"

OSM_RADIUS   = 10000
OSM_TIMEOUT  = 60
MAX_WORKERS  = 2       # keep at 2 — Overpass bans aggressive scrapers
MIN_DELAY    = 2.0     # seconds between requests PER WORKER
RETRY_DELAY  = 15      # base wait on 429 (doubles each retry)
MAX_RETRIES  = 3

# Store log10-normalised counts alongside raw counts?
# True  → output columns:  beaches, beaches_log, surf_spots, surf_spots_log …
# False → raw counts only
SAVE_LOG_COLS = True

# ─── POI DEFINITIONS ────────────────────────────────────
# Each entry: (set_name, overpass_filter, element_type)
# element_type: "node", "way", "relation", or "nwr" (all three)
#
# Vibe dimensions captured:
#   🌊 Coastal/Water  🏔️ Mountain/Adventure  🌲 Nature/Outdoor
#   🥾 Active Sports  🎭 Culture             🍽️ Social Life
#   🏙️ Urban baseline

OSM_POI_DEFS = [

    # ── 🌊 COASTAL VIBE ──────────────────────────────────────────────────────
    # coastline: the actual ocean shoreline — most reliable coastal signal in OSM.
    # A city with 0 coastline segments is definitively landlocked regardless of
    # what other water tags bleed in (lakes, reservoirs, rivers).
    ("coastline",      'way["natural"="coastline"]',                    "way"),
    ("beaches",        'way["natural"="beach"]',                        "way"),
    ("surf_spots",     'node["sport"="surfing"]',                       "node"),
    ("dive_centres",   'node["amenity"="dive_centre"]',                 "node"),
    ("marinas",        'nwr["leisure"="marina"]',                       "nwr"),

    # ── 🏔️ MOUNTAIN / TERRAIN ────────────────────────────────────────────────
    # peaks + viewpoints together = "there is terrain worth looking at"
    ("peaks",          'node["natural"="peak"]',                        "node"),
    ("viewpoints",     'node["tourism"="viewpoint"]',                   "node"),
    ("ski_areas",      'nwr["landuse"="winter_sports"]',                "nwr"),

    # ── 🌿 NATURE / OUTDOOR ──────────────────────────────────────────────────
    # nwr catches named park polygons, not just centroid nodes
    ("parks",          'nwr["leisure"="park"]',                         "nwr"),
    ("nature_reserves",'nwr["leisure"="nature_reserve"]',               "nwr"),
    ("climbing_spots", 'node["sport"="climbing"]',                      "node"),
    # cycling relations = named routes (e.g. EuroVelo), not every bike lane
    ("cycling_routes", 'relation["route"="bicycle"]',                   "relation"),

    # ── 🍜 FOOD CULTURE (quality signal, not quantity) ────────────────────────
    # street_food: only tagged when genuinely street/hawker — Bangkok/Hanoi pop
    ("street_food",    'node["cuisine"="street_food"]',                 "node"),
    # food_courts: hawker centres, market halls — SE Asia / Middle East signal
    ("food_courts",    'node["amenity"="food_court"]',                  "node"),
    # food_markets: Boqueria, Borough Market, Naschmarkt type density
    ("food_markets",   'nwr["amenity"="marketplace"]["market"="food"]', "nwr"),
    # bakeries: density separates European bread-culture cities clearly
    ("bakeries",       'node["shop"="bakery"]',                         "node"),
    # kept but log-scaled downstream — still useful as relative urban density
    ("restaurants",    'node["amenity"="restaurant"]',                  "node"),

    # ── 🌙 NIGHTLIFE ─────────────────────────────────────────────────────────
    # nightclubs: rare & intentional — Ibiza/Berlin/Bangkok vs Zurich
    ("nightclubs",     'node["amenity"="nightclub"]',                   "node"),
    ("bars",           'node["amenity"="bar"]',                         "node"),
    # live music: venues with live_music=yes or amenity=music_venue
    ("live_music",     'node["amenity"="music_venue"]',                 "node"),

    # ── 🎭 CULTURE ───────────────────────────────────────────────────────────
    ("museums",        'node["tourism"="museum"]',                      "node"),
    ("galleries",      'node["tourism"="gallery"]',                     "node"),
    # historic=* (any value) — castles, ruins, monuments, memorials
    ("historic_sites", 'node["historic"]',                              "node"),
    ("arts_centres",   'node["amenity"="arts_centre"]',                 "node"),

    # ── 🏙️ URBAN BASELINE ────────────────────────────────────────────────────
    # hotels: useful livability contrast, not a vibe signal on its own
    ("hotels",         'node["tourism"="hotel"]',                       "node"),

]
# NOTE on removed tags:
#   hiking_paths (way["highway"="path"]) — every urban footpath counts, pure noise
#   cafes (amenity=cafe)                 — same coverage bias as restaurants
#   attractions (tourism=attraction)     — too generic, overlaps historic + museum
#   shopping_malls (shop=mall)           — low OSM coverage, unreliable signal
#   theatres (amenity=theatre)           — subsumed by arts_centres which is broader

POI_NAMES = [name for name, _, _ in OSM_POI_DEFS]

# Minimum total raw POI count across a dimension's components before we
# trust the score. Below this threshold the score is suppressed to 0.0.
# Prevents a city with 3 nightclubs from scoring disproportionately high
# due to log-scale compression of small numbers.
MIN_EVIDENCE = {
    "vibe_coastal":   3,   # at least 3 coastal features total
    "vibe_mountain":  5,   # at least 5 peaks/viewpoints/etc
    "vibe_nature":    5,
    "vibe_food":      10,  # food scene needs real density
    "vibe_nightlife": 5,   # 3 bars in a city of 500k is noise
    "vibe_culture":   5,
}

# ── COMPOSITE SCORES ─────────────────────────────────────────────────────────
# Computed after fetching, written as extra columns.
# Each is a simple sum of log-normalised component scores (0–1 range each).
# Formula: sum(log10(x+1) / log10(max_expected+1)) per component
# max_expected values are generous upper bounds to keep scores in [0,1].

COMPOSITE_SCORES = {
    # coastline is a GATE: if coastline == 0 the whole score is forced to 0.
    # This kills Nanyang/Dongguan false positives from inland lakes/reservoirs.
    # The remaining components then score the richness of the coastal offering.
    "vibe_coastal":    [("coastline", 200), ("beaches", 50), ("surf_spots", 30), ("dive_centres", 20), ("marinas", 15)],
    "vibe_mountain":   [("peaks", 100), ("viewpoints", 80), ("ski_areas", 10), ("climbing_spots", 20)],
    "vibe_nature":     [("parks", 200), ("nature_reserves", 30), ("cycling_routes", 50)],
    "vibe_food":       [("street_food", 50), ("food_courts", 30), ("food_markets", 10), ("bakeries", 200), ("restaurants", 5000)],
    "vibe_nightlife":  [("nightclubs", 100), ("bars", 1000), ("live_music", 50)],
    "vibe_culture":    [("museums", 100), ("galleries", 80), ("historic_sites", 500), ("arts_centres", 30)],
}

def composite_score(pois_log: dict, components: list, raw_counts: dict = None, score_name: str = "") -> float:
    """Weighted sum of log-normalised counts, capped at 1.0 per component.

    Special case — vibe_coastal:
      If raw coastline count is 0, the city is landlocked and the entire
      coastal score is forced to 0.0, regardless of marinas/beaches from
      inland water bodies bleeding in.
    """
    # Hard zero gate for coastal: no coastline = not a coastal city
    if score_name == "vibe_coastal" and raw_counts is not None:
        if raw_counts.get("coastline", 0) == 0:
            return 0.0

    total = 0.0
    for poi_name, max_expected in components:
        raw_log  = pois_log.get(f"{poi_name}_log", 0.0)
        ceiling  = math.log10(max_expected + 1)
        total   += min(raw_log / ceiling, 1.0) if ceiling > 0 else 0.0
    return round(total / len(components), 4)   # normalise to [0, 1]


# ─── BUILD QUERY ─────────────────────────────────────────

def build_count_query(lat, lng):
    """
    ONE Overpass request per city.
    Stacks N blocks of  ( filter(around:R,lat,lng); ); out count;
    Overpass concatenates all 'out count' elements into one JSON array.
    """
    parts = []
    for _, osm_filter, _ in OSM_POI_DEFS:
        parts.append(
            f"  (\n    {osm_filter}(around:{OSM_RADIUS},{lat},{lng});\n  );\n  out count;"
        )
    return f"[out:json][timeout:{OSM_TIMEOUT}];\n" + "\n".join(parts)


# ─── LOG NORMALISATION ───────────────────────────────────

def log_norm(count):
    """log10(count+1) rounded to 3 dp. Compresses Seoul:15000 vs Quito:50."""
    if count < 0:
        return -1.0
    return round(math.log10(count + 1), 3)


# ─── PER-WORKER RATE LIMITER ─────────────────────────────
# Each thread tracks its own last-request time so workers don't
# fire simultaneously and blow through Overpass's slot budget.

_last_request: dict[int, float] = {}
_rate_lock = threading.Lock()

def _throttle():
    tid = threading.get_ident()
    with _rate_lock:
        last = _last_request.get(tid, 0)
        wait = MIN_DELAY - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        _last_request[tid] = time.time()


# ─── FETCH POIs FOR ONE CITY ─────────────────────────────

_write_lock = threading.Lock()

def fetch_city_pois(lat, lng):
    """Makes ONE Overpass API call. Returns {poi_name: count} or None."""
    url   = "https://overpass-api.de/api/interpreter"
    query = build_count_query(lat, lng)

    for attempt in range(MAX_RETRIES + 1):
        _throttle()
        try:
            r = requests.post(
                url,
                data={"data": query},
                timeout=OSM_TIMEOUT + 10,
                headers={"User-Agent": "CityVibeExtractor/2.1 (research)"}
            )

            if r.status_code in (429, 503, 504):
                wait = RETRY_DELAY * (2 ** attempt)   # 15 → 30 → 60 → 120 s
                print(f"    ⚠ HTTP {r.status_code}, backing off {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue

            if r.status_code != 200:
                print(f"    ✗ HTTP {r.status_code}")
                return None

            data     = r.json()
            elements = data.get("elements", [])
            counts   = {}
            count_els = [e for e in elements if e.get("type") == "count"]

            for i, (name, _, _) in enumerate(OSM_POI_DEFS):
                if i < len(count_els):
                    counts[name] = int(count_els[i].get("tags", {}).get("total", 0))
                else:
                    counts[name] = -1

            return counts

        except requests.exceptions.Timeout:
            wait = RETRY_DELAY * (2 ** attempt)
            print(f"    ⚠ Timeout, backing off {wait}s (attempt {attempt+1})")
            time.sleep(wait)
        except Exception as e:
            print(f"    ✗ Unexpected error: {e}")
            return None

    print("    ✗ All retries exhausted")
    return None


# ─── CSV HELPERS ─────────────────────────────────────────

def _fieldnames():
    base = ["city_name", "country_name", "continent", "lat", "lng", "population"]
    for name in POI_NAMES:
        base.append(name)
        if SAVE_LOG_COLS:
            base.append(f"{name}_log")
    base += list(COMPOSITE_SCORES.keys())
    return base

def init_csv():
    if not Path(OUTPUT_CSV).exists():
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_fieldnames()).writeheader()

def load_done():
    done = set()
    if Path(OUTPUT_CSV).exists():
        df = pd.read_csv(OUTPUT_CSV)
        if "city_name" in df.columns:
            done = set(df["city_name"].dropna())
    return done


# ─── WORKER ──────────────────────────────────────────────

def process_city(row):
    city        = row["city_name"]
    lat, lng    = row["lat"], row["lng"]
    pois        = fetch_city_pois(lat, lng)
    if pois is None:
        return None, city

    row_out = {
        "city_name":    city,
        "country_name": row["country_name"],
        "continent":    row["continent"],
        "lat":          lat,
        "lng":          lng,
        "population":   row["population"],
    }

    # raw + log columns
    pois_log = {}
    for name in POI_NAMES:
        raw = pois.get(name, -1)
        row_out[name] = raw
        lv = log_norm(raw)
        if SAVE_LOG_COLS:
            row_out[f"{name}_log"] = lv
        pois_log[f"{name}_log"] = lv

    # composite vibe scores
    for score_name, components in COMPOSITE_SCORES.items():
        # Evidence gate: if total raw count across components is below threshold,
        # suppress the score — avoids inflating small cities with sparse OSM data.
        total_evidence = sum(pois.get(poi_name, 0) for poi_name, _ in components if pois.get(poi_name, -1) >= 0)
        min_ev = MIN_EVIDENCE.get(score_name, 0)
        if total_evidence < min_ev:
            row_out[score_name] = 0.0
        else:
            row_out[score_name] = composite_score(pois_log, components, raw_counts=pois, score_name=score_name)

    return row_out, city


# ─── MAIN ────────────────────────────────────────────────

def run():
    df      = pd.read_csv(INPUT_CSV)
    done    = load_done()
    pending = df[~df["city_name"].isin(done)].to_dict("records")

    print(f"Already processed : {len(done)} cities")
    print(f"Pending           : {len(pending)} cities")
    print(f"Workers           : {MAX_WORKERS}  |  min delay/worker: {MIN_DELAY}s")
    print(f"Queries per city  : 1 (batched)  |  POI types: {len(OSM_POI_DEFS)}")
    print(f"Log columns       : {SAVE_LOG_COLS}\n")

    init_csv()
    fields = _fieldnames()

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_city, row): row for row in pending}

            for future in as_completed(futures):
                row_out, city = future.result()

                if row_out is None:
                    print(f"  ✗ FAILED: {city}")
                    continue

                with _write_lock:
                    writer.writerow(row_out)
                    f.flush()

                # Console: show top composite vibes
                vibes = sorted(
                    [(k, row_out[k]) for k in COMPOSITE_SCORES if row_out.get(k, 0) > 0],
                    key=lambda x: -x[1]
                )[:3]
                vibe_str = ", ".join(f"{k.replace('vibe_','')}:{v:.2f}" for k, v in vibes)
                print(f"  ✔ {city} — {vibe_str}")

    print("\nDone ✓")


if __name__ == "__main__":
    run()