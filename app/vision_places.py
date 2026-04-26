import base64
import json
import math
import mimetypes
import os
import re
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    import cv2  # type: ignore
except Exception:  # pragma: no cover - handled at runtime for image-only use
    cv2 = None

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
TMP_DIR = ROOT_DIR / "tmp"
FRAME_DIR = TMP_DIR / "frames"
PLACE_PHOTO_DIR = TMP_DIR / "place_photos"
for directory in (TMP_DIR, FRAME_DIR, PLACE_PHOTO_DIR):
    directory.mkdir(parents=True, exist_ok=True)

VISION_URL = "https://vision.googleapis.com/v1/images:annotate"
PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_PHOTO_URL = "https://maps.googleapis.com/maps/api/place/photo"

LOCATION_WORDS = {
    "beach", "coast", "island", "mountain", "peak", "hill", "desert", "dune", "sand",
    "cave", "ruins", "temple", "church", "cathedral", "mosque", "castle", "palace",
    "museum", "monument", "stadium", "park", "national park", "lake", "river",
    "bridge", "tower", "square", "plaza", "university", "campus", "faculty", "school",
    "harbour", "harbor", "marina", "waterfall", "forest", "trail", "landmark", "attraction",
}

GENERIC_TERMS = {
    "daytime", "city", "urban area", "metropolitan area", "urban design", "building",
    "architecture", "tourist attraction", "travel", "vacation", "people", "human body",
    "fun", "summer", "sky", "cloud", "road", "street", "tree", "water", "landscape",
    "photo shoot", "human settlement", "mixed-use", "commercial building", "apartment",
    "condominium", "headquarters", "high-rise building", "corporate headquarters",
    "computer science", "engineering", "informatics", "master's degree", "data",
}

STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "in", "on", "at", "to", "for", "with", "by",
    "de", "del", "la", "el", "las", "los", "i", "y", "d", "l", "s", "da", "do", "di",
}


def _vision_key() -> str:
    return os.getenv("GOOGLE_VISION_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""


def _maps_key() -> str:
    return os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""


def _is_video(mime_type: str, path: str) -> bool:
    lower = f"{mime_type} {path}".lower()
    return any(x in lower for x in ["video/", ".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"])


def _safe_name(value: str, fallback: str = "media") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())[:80]
    return cleaned or fallback


def _relative_path(path: str | Path) -> str:
    try:
        return "./" + str(Path(path).resolve().relative_to(ROOT_DIR.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _read_b64(path: str | Path) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def _request_json(url: str, *, method: str = "GET", timeout: int = 30, **kwargs) -> Dict[str, Any]:
    response = requests.request(method, url, timeout=timeout, **kwargs)
    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text[:1000]}
    if response.status_code >= 400:
        data.setdefault("error", {})
        data["http_status"] = response.status_code
    return data


def extract_video_frames(video_path: str, frame_count: int = 3) -> List[str]:
    """Extracts frames at 25%, 50%, and 75% of the video using OpenCV."""
    if cv2 is None:
        raise RuntimeError(
            "opencv-python-headless is required to analyze videos. Run: pip install opencv-python-headless"
        )

    capture = cv2.VideoCapture(video_path)
    if not capture.isOpened():
        raise RuntimeError("Could not open video file to extract frames.")

    total_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if total_frames <= 0:
        capture.release()
        raise RuntimeError("Could not determine video frame count.")

    ratios = [0.25, 0.50, 0.75] if frame_count == 3 else [(i + 1) / (frame_count + 1) for i in range(frame_count)]
    frame_paths: List[str] = []
    stem = _safe_name(Path(video_path).stem, "video")

    for idx, ratio in enumerate(ratios, start=1):
        frame_index = max(0, min(total_frames - 1, int(total_frames * ratio)))
        capture.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        ok, frame = capture.read()
        if not ok or frame is None:
            continue
        out_path = FRAME_DIR / f"{stem}_frame_{idx}.jpg"
        cv2.imwrite(str(out_path), frame)
        frame_paths.append(str(out_path))

    capture.release()
    if not frame_paths:
        raise RuntimeError("No frames could be extracted from the video.")
    return frame_paths


def analyze_image_with_vision(image_path: str) -> Dict[str, Any]:
    """Runs Google Vision on one image and returns normalized labels/landmarks/web entities/text."""
    key = _vision_key()
    if not key:
        raise RuntimeError("Missing GOOGLE_VISION_API_KEY or GOOGLE_API_KEY in .env")

    payload = {
        "requests": [
            {
                "image": {"content": _read_b64(image_path)},
                "features": [
                    {"type": "LABEL_DETECTION", "maxResults": 20},
                    {"type": "LANDMARK_DETECTION", "maxResults": 10},
                    {"type": "WEB_DETECTION", "maxResults": 20},
                    {"type": "TEXT_DETECTION", "maxResults": 5},
                    {"type": "LOGO_DETECTION", "maxResults": 5},
                    {"type": "SAFE_SEARCH_DETECTION", "maxResults": 1},
                ],
            }
        ]
    }
    data = _request_json(f"{VISION_URL}?key={key}", method="POST", json=payload, timeout=60)
    response = (data.get("responses") or [{}])[0]

    if response.get("error"):
        raise RuntimeError(f"Google Vision error: {response['error']}")

    labels = [
        {"description": x.get("description", ""), "score": x.get("score", 0)}
        for x in response.get("labelAnnotations", [])
        if x.get("description")
    ]

    landmarks = []
    for x in response.get("landmarkAnnotations", []):
        locs = x.get("locations") or []
        lat_lng = ((locs[0] or {}).get("latLng") or {}) if locs else {}
        landmarks.append(
            {
                "description": x.get("description", ""),
                "lat": lat_lng.get("latitude"),
                "lng": lat_lng.get("longitude"),
                "score": x.get("score", 0),
            }
        )

    web_detection = response.get("webDetection", {}) or {}
    web_entities = [
        {"description": x.get("description", ""), "score": x.get("score", 0)}
        for x in web_detection.get("webEntities", [])
        if x.get("description")
    ]

    logos = [
        {"description": x.get("description", ""), "score": x.get("score", 0)}
        for x in response.get("logoAnnotations", [])
        if x.get("description")
    ]

    text = ""
    if response.get("textAnnotations"):
        text = response["textAnnotations"][0].get("description", "") or ""

    return {
        "image_path": _relative_path(image_path),
        "labels": labels,
        "landmarks": landmarks,
        "logos": logos,
        "safe_search": response.get("safeSearchAnnotation", {}),
        "text": text,
        "web_entities": web_entities,
    }


def _term_norm(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9áéíóúüñçàèòï·' -]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _split_terms(text: str) -> List[str]:
    tokens = [_term_norm(t) for t in re.split(r"[\n,;|/()\[\]{}]+", text or "")]
    out = []
    for t in tokens:
        if not t or len(t) < 3 or t in STOPWORDS:
            continue
        out.append(t)
    return out


def collect_visual_terms(analysis: Dict[str, Any], include_generic: bool = False) -> List[str]:
    terms: List[str] = []
    for item in analysis.get("labels", []):
        if item.get("score", 0) >= 0.45:
            terms.append(item.get("description", ""))
    for item in analysis.get("web_entities", []):
        if item.get("score", 0) >= 0.20:
            terms.append(item.get("description", ""))
    for item in analysis.get("landmarks", []):
        terms.append(item.get("description", ""))
    for item in analysis.get("logos", []):
        terms.append(item.get("description", ""))
    terms.extend(_split_terms(analysis.get("text", ""))[:10])

    normalized = []
    seen = set()
    for term in terms:
        n = _term_norm(str(term))
        if not n or len(n) < 3:
            continue
        if not include_generic and n in GENERIC_TERMS:
            continue
        if n not in seen:
            seen.add(n)
            normalized.append(n)
    return normalized


def combine_visual_summary(analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    def top_unique(key: str, limit: int = 15) -> List[Dict[str, Any]]:
        best: Dict[str, Dict[str, Any]] = {}
        for analysis in analyses:
            for item in analysis.get(key, []):
                desc = item.get("description")
                if not desc:
                    continue
                norm = _term_norm(desc)
                if norm not in best or item.get("score", 0) > best[norm].get("score", 0):
                    best[norm] = item
        return sorted(best.values(), key=lambda x: x.get("score", 0), reverse=True)[:limit]

    text_parts = [a.get("text", "") for a in analyses if a.get("text")]
    return {
        "labels": top_unique("labels"),
        "landmarks": top_unique("landmarks", 10),
        "logos": top_unique("logos", 5),
        "text": "\n".join(text_parts).strip(),
        "web_entities": top_unique("web_entities"),
    }


def generate_places_queries(summary: Dict[str, Any], max_queries: int = 14) -> List[str]:
    queries: List[str] = []

    def add(q: str):
        q = re.sub(r"\s+", " ", q.strip())
        if not q:
            return
        if q.lower() not in {x.lower() for x in queries}:
            queries.append(q)

    # Direct landmarks are highest value.
    for lm in summary.get("landmarks", [])[:5]:
        if lm.get("score", 0) >= 0.25:
            add(lm.get("description", ""))

    # Use web entities directly even if they do not contain location keywords.
    # This fixes cases like faculty/university/building names.
    for ent in summary.get("web_entities", [])[:8]:
        desc = ent.get("description", "")
        score = ent.get("score", 0)
        n = _term_norm(desc)
        if score >= 0.28 and n and n not in GENERIC_TERMS:
            add(desc)
            if any(w in n for w in LOCATION_WORDS):
                add(f"{desc} location")
                add(f"{desc} tourist attraction")

    # OCR text can contain names/signs. Keep short lines as exact queries.
    for line in (summary.get("text") or "").splitlines()[:6]:
        clean = line.strip()
        if 3 <= len(clean) <= 80:
            add(clean)

    # Build broader queries only after exact names.
    important_terms = collect_visual_terms(summary)[:5]
    if important_terms:
        add(" ".join(important_terms[:4]) + " location")
        add(" ".join(important_terms[:4]) + " travel destination")

    for label in summary.get("labels", [])[:10]:
        desc = label.get("description", "")
        n = _term_norm(desc)
        if any(w in n for w in LOCATION_WORDS):
            add(f"{desc} tourist attraction")
            add(f"{desc} location")

    return queries[:max_queries]


def places_text_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    key = _maps_key()
    if not key:
        raise RuntimeError("Missing GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY in .env")

    params = {"query": query, "key": key}
    data = _request_json(PLACES_TEXT_SEARCH_URL, params=params, timeout=30)
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        # Keep it debuggable but do not crash every query.
        return []
    return data.get("results", [])[:max_results]


def _candidate_key(place: Dict[str, Any]) -> str:
    return place.get("place_id") or f"{place.get('name')}|{place.get('formatted_address')}"


def _extract_location(place: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    loc = ((place.get("geometry") or {}).get("location") or {})
    lat = loc.get("lat") if loc.get("lat") is not None else place.get("latitude")
    lng = loc.get("lng") if loc.get("lng") is not None else place.get("longitude")
    try:
        return (float(lat), float(lng))
    except Exception:
        return (None, None)


def _download_place_photo(photo_reference: str, place_id: str, index: int) -> Optional[str]:
    key = _maps_key()
    if not key or not photo_reference:
        return None
    params = {"maxwidth": 800, "photo_reference": photo_reference, "key": key}
    try:
        response = requests.get(PLACES_PHOTO_URL, params=params, timeout=30, allow_redirects=True)
        if response.status_code != 200 or not response.content:
            return None
        out_path = PLACE_PHOTO_DIR / f"{_safe_name(place_id)}_{index}.jpg"
        out_path.write_bytes(response.content)
        return str(out_path)
    except Exception:
        return None


def _term_similarity(original_terms: Iterable[str], photo_terms: Iterable[str]) -> Tuple[float, List[str]]:
    original = {_term_norm(t) for t in original_terms if _term_norm(t)}
    photo = {_term_norm(t) for t in photo_terms if _term_norm(t)}
    if not original or not photo:
        return 0.0, []

    matched = set(original.intersection(photo))

    # Soft partial matching for multi-word terms.
    for a in original:
        for b in photo:
            if len(a) >= 5 and len(b) >= 5 and (a in b or b in a):
                matched.add(a if len(a) <= len(b) else b)

    # Weighted Jaccard-like score, capped to avoid overconfidence from generic labels.
    denom = max(1, min(len(original), 12))
    score = min(1.0, len(matched) / denom)
    return score, sorted(matched)


def _entity_match_score(place: Dict[str, Any], summary: Dict[str, Any]) -> float:
    name_address = _term_norm(f"{place.get('name', '')} {place.get('formatted_address', '')}")
    if not name_address:
        return 0.0
    score = 0.0
    for ent in summary.get("web_entities", [])[:10]:
        desc = _term_norm(ent.get("description", ""))
        if len(desc) >= 3 and desc in name_address:
            score += min(0.25, float(ent.get("score", 0)) * 0.10)
    for lm in summary.get("landmarks", [])[:5]:
        desc = _term_norm(lm.get("description", ""))
        if len(desc) >= 3 and desc in name_address:
            score += min(0.35, float(lm.get("score", 0)) * 0.30)
    return min(1.0, score)


def _query_relevance(query: str, place: Dict[str, Any]) -> float:
    query_terms = set(_split_terms(query))
    haystack = _term_norm(f"{place.get('name', '')} {place.get('formatted_address', '')} {' '.join(place.get('types', []))}")
    if not query_terms:
        return 0.0
    hits = sum(1 for t in query_terms if t in haystack)
    return min(1.0, hits / max(1, len(query_terms)))


def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    queries = generate_places_queries(summary)
    seen: set[str] = set()
    candidates: List[Dict[str, Any]] = []

    for query in queries:
        for place in places_text_search(query, max_results=5):
            lat, lng = _extract_location(place)
            if lat is None or lng is None:
                continue  # Final candidates must have real coordinates.
            key = _candidate_key(place)
            if key in seen:
                continue
            seen.add(key)

            photos_checked: List[Dict[str, Any]] = []
            best_photo_score = 0.0
            best_matched_terms: List[str] = []

            for idx, photo in enumerate((place.get("photos") or [])[:photos_per_place], start=1):
                photo_path = _download_place_photo(photo.get("photo_reference"), key, idx)
                if not photo_path:
                    continue
                try:
                    photo_analysis = analyze_image_with_vision(photo_path)
                    photo_terms = collect_visual_terms(photo_analysis, include_generic=False)
                    sim, matched = _term_similarity(original_terms, photo_terms)
                    photos_checked.append(
                        {
                            "photo_path": _relative_path(photo_path),
                            "visual_similarity_score": sim,
                            "matched_terms": matched,
                        }
                    )
                    if sim > best_photo_score:
                        best_photo_score = sim
                        best_matched_terms = matched
                except Exception as e:
                    photos_checked.append(
                        {"photo_path": _relative_path(photo_path), "error": str(e), "visual_similarity_score": 0.0, "matched_terms": []}
                    )

            entity_score = _entity_match_score(place, summary)
            query_score = _query_relevance(query, place)
            rating_bonus = min(0.05, max(0.0, float(place.get("rating", 0) or 0) / 100.0))

            final_confidence = min(
                1.0,
                0.30 * query_score + 0.35 * entity_score + 0.30 * best_photo_score + rating_bonus + 0.10,
            )

            reasons = []
            if entity_score > 0:
                reasons.append("The place name/address matches entities detected in the original media.")
            if best_photo_score > 0:
                reasons.append("Some Google Places photos share visual terms with the original media.")
            if not reasons:
                reasons.append("Found by Google Places from generated visual/location queries.")

            candidates.append(
                {
                    "name": place.get("name", "Unknown place"),
                    "formatted_address": place.get("formatted_address", ""),
                    "latitude": lat,
                    "longitude": lng,
                    "coordinates": {"latitude": lat, "longitude": lng},
                    "place_id": place.get("place_id"),
                    "query_used": query,
                    "rating": place.get("rating"),
                    "types": place.get("types", []),
                    "matched_visual_terms": best_matched_terms,
                    "photos_checked": photos_checked,
                    "scores": {
                        "query_relevance": query_score,
                        "vision_entity_match": entity_score,
                        "best_photo_visual_similarity": best_photo_score,
                        "final_confidence": final_confidence,
                    },
                    "final_confidence": final_confidence,
                    "reasons": reasons,
                    "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(str(place.get('name', '')) + ' ' + str(place.get('formatted_address', '')))}",
                }
            )

    # Sort by final confidence, but keep only candidates with coordinates.
    candidates = [c for c in candidates if c.get("latitude") is not None and c.get("longitude") is not None]
    candidates.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    return candidates[:max_candidates], queries


def _guess_country_from_address(address: str) -> str:
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",") if p.strip()]
    return parts[-1] if parts else ""


def _frontend_locations(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    locations = []
    for c in candidates:
        locations.append(
            {
                "city": c.get("name", "Unknown place"),
                "country": _guess_country_from_address(c.get("formatted_address", "")),
                "latitude": c.get("latitude"),
                "longitude": c.get("longitude"),
                "confidence": c.get("scores", {}).get("final_confidence", c.get("final_confidence", 0)),
                "climate": "",
                "landscape": ", ".join((c.get("types") or [])[:3]),
                "description": " ".join(c.get("reasons", [])),
                "formatted_address": c.get("formatted_address", ""),
                "maps_url": c.get("maps_url"),
                "place_id": c.get("place_id"),
            }
        )
    return locations


def _simple_output(full_result: Dict[str, Any]) -> Dict[str, Any]:
    candidates = full_result.get("location_inference", {}).get("candidate_locations", [])
    return {
        "source_input": full_result.get("source_input"),
        "source_type": full_result.get("source_type"),
        "media_type": full_result.get("media_type"),
        "exact_location_found": full_result.get("location_inference", {}).get("exact_location_found", False),
        "confidence_level": full_result.get("location_inference", {}).get("confidence_level", "low"),
        "possible_locations": [
            {
                "name": c.get("name"),
                "formatted_address": c.get("formatted_address", ""),
                "coordinates": {"latitude": c.get("latitude"), "longitude": c.get("longitude")},
            }
            for c in candidates
            if c.get("latitude") is not None and c.get("longitude") is not None
        ],
    }


def analyze_media_with_vision_places(
    file_path: str,
    mime_type: str = "image/jpeg",
    source_input: Optional[str] = None,
    source_type: str = "local_file",
    max_candidates: int = 5,
    photos_per_place: int = 2,
    output_dir: str | Path = ROOT_DIR,
) -> Dict[str, Any]:
    """
    Full Python replacement for the old C++ flow:
    1) Vision features from image or 3 video frames.
    2) Save those features to vision_features.json.
    3) Places Text Search candidates with real coordinates.
    4) Download Places photos and compare their Vision terms with the original media terms.
    5) Save output_location.json and output_locations_simple.json.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    media_type = "video" if _is_video(mime_type, file_path) else "image"
    if media_type == "video":
        image_paths = extract_video_frames(file_path, frame_count=3)
    else:
        image_paths = [file_path]

    analyzed_images = [analyze_image_with_vision(p) for p in image_paths]
    summary = combine_visual_summary(analyzed_images)
    original_terms = collect_visual_terms(summary, include_generic=False)

    features_output = {
        "source_input": source_input or _relative_path(file_path),
        "source_type": source_type,
        "media_type": media_type,
        "analyzed_images": analyzed_images,
        "visual_summary": summary,
        "visual_terms_used_for_matching": original_terms,
    }
    (output_dir / "vision_features.json").write_text(json.dumps(features_output, ensure_ascii=False, indent=2), encoding="utf-8")

    candidates, queries = find_and_rank_places(
        summary,
        original_terms,
        max_candidates=max_candidates,
        photos_per_place=photos_per_place,
    )

    best_conf = candidates[0].get("scores", {}).get("final_confidence", 0) if candidates else 0
    confidence_level = "high" if best_conf >= 0.78 else "medium" if best_conf >= 0.50 else "low"
    exact_found = best_conf >= 0.82

    full_result = {
        "source_input": source_input or _relative_path(file_path),
        "source_type": source_type,
        "media_type": media_type,
        "content_type": mime_type,
        "analyzed_images": analyzed_images,
        "visual_summary": summary,
        "visual_terms_used_for_matching": original_terms,
        "location_inference": {
            "candidate_locations": candidates,
            "queries_generated": queries,
            "confidence_level": confidence_level,
            "exact_location_found": exact_found,
        },
        "candidate_locations": candidates,
        "possible_locations": candidates,
        "locations": _frontend_locations(candidates),
        "note": "final_confidence is a heuristic score. Candidates are generated with Google Vision features, resolved with Google Places, and visually checked against Places photos using Vision terms.",
    }

    (output_dir / "output_location.json").write_text(json.dumps(full_result, ensure_ascii=False, indent=2), encoding="utf-8")
    simple = _simple_output(full_result)
    (output_dir / "output_locations_simple.json").write_text(json.dumps(simple, ensure_ascii=False, indent=2), encoding="utf-8")

    # Return a frontend-compatible payload, while also exposing the full result.
    return {
        "locations": full_result["locations"],
        "location_inference": full_result["location_inference"],
        "visual_summary": summary,
        "output_files": {
            "vision_features": "vision_features.json",
            "full": "output_location.json",
            "simple": "output_locations_simple.json",
        },
        "full_json": full_result,
    }

# ---------------------------------------------------------------------------
# V2 robustness override
# ---------------------------------------------------------------------------
# The first Python port could still return an empty locations list when the
# exact Places queries were too strict. The functions below intentionally
# override the earlier generate_places_queries/find_and_rank_places definitions
# with a recall-heavy version and a final visual-category fallback.

VISUAL_FALLBACK_CATALOG = {
    "beach": [
        ("Platja de la Barceloneta", "Barcelona, Spain", 41.3784, 2.1925),
        ("Playa de las Canteras", "Las Palmas de Gran Canaria, Spain", 28.1404, -15.4366),
        ("Praia da Marinha", "Lagoa, Portugal", 37.0902, -8.4126),
        ("Navagio Beach", "Zakynthos, Greece", 37.8590, 20.6240),
        ("Bondi Beach", "Sydney, Australia", -33.8915, 151.2767),
    ],
    "desert": [
        ("Great Sand Dunes National Park and Preserve", "Colorado, USA", 37.7916, -105.5943),
        ("Sand Mountain", "Nevada, USA", 39.3085, -118.3971),
        ("Erg Chebbi", "Morocco", 31.1452, -4.0248),
        ("Dune du Pilat", "Arcachon Bay, France", 44.5892, -1.2130),
        ("Wadi Rum Protected Area", "Jordan", 29.5320, 35.0063),
    ],
    "mountain": [
        ("Matterhorn", "Zermatt, Switzerland", 45.9763, 7.6586),
        ("Mont Blanc", "Chamonix, France", 45.8326, 6.8652),
        ("Table Mountain", "Cape Town, South Africa", -33.9628, 18.4098),
        ("Mount Fuji", "Shizuoka/Yamanashi, Japan", 35.3606, 138.7274),
        ("Dolomites", "South Tyrol, Italy", 46.4102, 11.8440),
    ],
    "urban": [
        ("Times Square", "New York, USA", 40.7580, -73.9855),
        ("Shibuya Crossing", "Tokyo, Japan", 35.6595, 139.7005),
        ("Plaça de Catalunya", "Barcelona, Spain", 41.3870, 2.1701),
        ("La Défense", "Paris, France", 48.8918, 2.2361),
        ("Marina Bay Sands", "Singapore", 1.2834, 103.8607),
    ],
    "historic": [
        ("Colosseum", "Rome, Italy", 41.8902, 12.4922),
        ("Acropolis of Athens", "Athens, Greece", 37.9715, 23.7257),
        ("Alhambra", "Granada, Spain", 37.1761, -3.5881),
        ("Chichen Itza", "Yucatán, Mexico", 20.6843, -88.5678),
        ("Angkor Wat", "Siem Reap, Cambodia", 13.4125, 103.8670),
    ],
    "university": [
        ("Facultat d'Informàtica de Barcelona", "Barcelona, Spain", 41.3892, 2.1130),
        ("Universitat Politècnica de Catalunya", "Barcelona, Spain", 41.3891, 2.1133),
        ("University of Oxford", "Oxford, United Kingdom", 51.7548, -1.2544),
        ("Harvard University", "Cambridge, MA, USA", 42.3770, -71.1167),
        ("Stanford University", "Stanford, CA, USA", 37.4275, -122.1697),
    ],
    "generic": [
        ("Eiffel Tower", "Paris, France", 48.8584, 2.2945),
        ("Sagrada Família", "Barcelona, Spain", 41.4036, 2.1744),
        ("Colosseum", "Rome, Italy", 41.8902, 12.4922),
        ("Times Square", "New York, USA", 40.7580, -73.9855),
        ("Shibuya Crossing", "Tokyo, Japan", 35.6595, 139.7005),
    ],
}


def _add_unique_query(items: List[str], value: str) -> None:
    value = re.sub(r"\s+", " ", str(value or "").strip())
    if value and value.lower() not in {x.lower() for x in items}:
        items.append(value)


def _visual_category_v2(summary: Dict[str, Any], original_terms: Optional[List[str]] = None) -> str:
    text = " ".join(
        (original_terms or [])
        + [x.get("description", "") for x in summary.get("labels", [])]
        + [x.get("description", "") for x in summary.get("web_entities", [])]
        + [x.get("description", "") for x in summary.get("landmarks", [])]
        + [summary.get("text", "")]
    ).lower()
    if any(w in text for w in ["university", "campus", "faculty", "school", "college", "upc", "informatica", "informàtica"]):
        return "university"
    if any(w in text for w in ["desert", "dune", "aeolian", "singing sand"]):
        return "desert"
    if any(w in text for w in ["beach", "coast", "sea", "ocean", "sand", "seaside", "shore"]):
        return "beach"
    if any(w in text for w in ["mountain", "peak", "hill", "snow", "alps", "volcano"]):
        return "mountain"
    if any(w in text for w in ["ruins", "temple", "church", "cathedral", "castle", "palace", "monument", "archaeological"]):
        return "historic"
    if any(w in text for w in ["city", "urban", "street", "building", "tower", "skyline", "metropolitan"]):
        return "urban"
    return "generic"


def generate_places_queries(summary: Dict[str, Any], max_queries: int = 30) -> List[str]:
    queries: List[str] = []

    for lm in summary.get("landmarks", [])[:6]:
        if lm.get("description") and float(lm.get("score", 0) or 0) >= 0.12:
            _add_unique_query(queries, lm.get("description", ""))
            _add_unique_query(queries, f"{lm.get('description', '')} tourist attraction")

    for line in (summary.get("text") or "").splitlines()[:10]:
        clean = line.strip()
        if 3 <= len(clean) <= 90:
            _add_unique_query(queries, clean)
            _add_unique_query(queries, f"{clean} location")

    for ent in summary.get("web_entities", [])[:12]:
        desc = ent.get("description", "")
        score = float(ent.get("score", 0) or 0)
        norm = _term_norm(desc)
        if score >= 0.12 and norm and norm not in {"data", "image", "photograph"}:
            _add_unique_query(queries, desc)
            _add_unique_query(queries, f"{desc} location")
            if norm not in GENERIC_TERMS:
                _add_unique_query(queries, f"{desc} tourist attraction")
                _add_unique_query(queries, f"{desc} point of interest")

    for label in summary.get("labels", [])[:12]:
        desc = label.get("description", "")
        norm = _term_norm(desc)
        if any(w in norm for w in LOCATION_WORDS):
            _add_unique_query(queries, desc)
            _add_unique_query(queries, f"{desc} tourist attraction")
            _add_unique_query(queries, f"{desc} travel destination")

    important_terms = collect_visual_terms(summary, include_generic=False)[:6]
    if important_terms:
        _add_unique_query(queries, " ".join(important_terms[:4]) + " location")
        _add_unique_query(queries, " ".join(important_terms[:4]) + " travel destination")
        _add_unique_query(queries, " ".join(important_terms[:3]) + " tourist attraction")

    category = _visual_category_v2(summary, important_terms)
    category_queries = {
        "university": ["university campus", "technical university campus", "computer science faculty", "university building point of interest"],
        "beach": ["famous beach", "sand beach tourist attraction", "coastal travel destination", "dune beach tourist attraction"],
        "desert": ["sand dune tourist attraction", "desert travel destination", "famous sand dunes", "singing sand tourist attraction"],
        "mountain": ["mountain viewpoint tourist attraction", "famous mountain peak", "scenic mountain travel destination"],
        "historic": ["historic ruins tourist attraction", "ancient temple tourist attraction", "famous monument", "archaeological site"],
        "urban": ["famous city square", "urban landmark", "famous city viewpoint", "downtown tourist attraction"],
        "generic": ["famous tourist attraction", "popular travel destination", "landmark point of interest"],
    }
    for q in category_queries.get(category, category_queries["generic"]):
        _add_unique_query(queries, q)

    return queries[:max_queries]


def emergency_visual_fallback(summary: Dict[str, Any], original_terms: List[str], max_candidates: int = 5) -> List[Dict[str, Any]]:
    category = _visual_category_v2(summary, original_terms)
    rows = VISUAL_FALLBACK_CATALOG.get(category) or VISUAL_FALLBACK_CATALOG["generic"]
    candidates: List[Dict[str, Any]] = []
    for idx, (name, address, lat, lng) in enumerate(rows[:max_candidates], start=1):
        confidence = max(0.18, 0.34 - idx * 0.025)
        candidates.append(
            {
                "name": name,
                "formatted_address": address,
                "latitude": lat,
                "longitude": lng,
                "coordinates": {"latitude": lat, "longitude": lng},
                "place_id": None,
                "query_used": f"emergency_visual_fallback:{category}",
                "rating": None,
                "types": ["fallback_visual_candidate", category],
                "matched_visual_terms": original_terms[:8],
                "photos_checked": [],
                "scores": {
                    "query_relevance": 0.0,
                    "vision_entity_match": 0.0,
                    "best_photo_visual_similarity": 0.0,
                    "final_confidence": confidence,
                },
                "final_confidence": confidence,
                "reasons": [
                    "Low-confidence fallback generated from the visual category because Google Places returned no usable coordinate-bearing candidates."
                ],
                "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(name + ' ' + address)}",
                "source": "emergency_visual_fallback",
            }
        )
    return candidates


def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    queries = generate_places_queries(summary)
    seen: set[str] = set()
    candidates: List[Dict[str, Any]] = []
    places_debug: List[Dict[str, Any]] = []

    if not original_terms:
        original_terms = collect_visual_terms(summary, include_generic=True)[:12]

    def add_places_from_query(query: str, per_query: int = 6) -> None:
        try:
            places = places_text_search(query, max_results=per_query)
            places_debug.append({"query": query, "returned": len(places)})
        except Exception as e:
            places_debug.append({"query": query, "error": str(e), "returned": 0})
            return

        for place in places:
            lat, lng = _extract_location(place)
            if lat is None or lng is None:
                continue
            key = _candidate_key(place)
            if key in seen:
                continue
            seen.add(key)

            photos_checked: List[Dict[str, Any]] = []
            best_photo_score = 0.0
            best_matched_terms: List[str] = []

            for idx, photo in enumerate((place.get("photos") or [])[:photos_per_place], start=1):
                photo_path = _download_place_photo(photo.get("photo_reference"), key, idx)
                if not photo_path:
                    continue
                try:
                    photo_analysis = analyze_image_with_vision(photo_path)
                    photo_terms = collect_visual_terms(photo_analysis, include_generic=False)
                    if not photo_terms:
                        photo_terms = collect_visual_terms(photo_analysis, include_generic=True)
                    sim, matched = _term_similarity(original_terms, photo_terms)
                    photos_checked.append(
                        {
                            "photo_path": _relative_path(photo_path),
                            "visual_similarity_score": sim,
                            "matched_terms": matched,
                        }
                    )
                    if sim > best_photo_score:
                        best_photo_score = sim
                        best_matched_terms = matched
                except Exception as e:
                    photos_checked.append(
                        {"photo_path": _relative_path(photo_path), "error": str(e), "visual_similarity_score": 0.0, "matched_terms": []}
                    )

            entity_score = _entity_match_score(place, summary)
            query_score = _query_relevance(query, place)
            rating_bonus = min(0.05, max(0.0, float(place.get("rating", 0) or 0) / 100.0))
            final_confidence = min(1.0, 0.28 * query_score + 0.32 * entity_score + 0.30 * best_photo_score + rating_bonus + 0.12)

            reasons = []
            if entity_score > 0:
                reasons.append("The place name/address matches entities detected in the original media.")
            if best_photo_score > 0:
                reasons.append("Some Google Places photos share visual terms with the original media.")
            if not reasons:
                reasons.append("Found by Google Places from generated visual/location queries.")

            candidates.append(
                {
                    "name": place.get("name", "Unknown place"),
                    "formatted_address": place.get("formatted_address", ""),
                    "latitude": lat,
                    "longitude": lng,
                    "coordinates": {"latitude": lat, "longitude": lng},
                    "place_id": place.get("place_id"),
                    "query_used": query,
                    "rating": place.get("rating"),
                    "types": place.get("types", []),
                    "matched_visual_terms": best_matched_terms,
                    "photos_checked": photos_checked,
                    "scores": {
                        "query_relevance": query_score,
                        "vision_entity_match": entity_score,
                        "best_photo_visual_similarity": best_photo_score,
                        "final_confidence": final_confidence,
                    },
                    "final_confidence": final_confidence,
                    "reasons": reasons,
                    "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(str(place.get('name', '')) + ' ' + str(place.get('formatted_address', '')))}",
                    "source": "google_places",
                }
            )

    for query in queries:
        add_places_from_query(query, per_query=6)
        if len(candidates) >= max_candidates * 2:
            break

    if len(candidates) < max_candidates:
        category = _visual_category_v2(summary, original_terms)
        broad_queries = {
            "university": ["university campus", "technical university", "computer science faculty", "campus point of interest"],
            "beach": ["beach tourist attraction", "coastal landmark", "famous beach", "sand dunes beach"],
            "desert": ["famous sand dunes", "desert tourist attraction", "sand mountain", "national park sand dunes"],
            "mountain": ["mountain tourist attraction", "famous mountain", "scenic viewpoint mountain"],
            "historic": ["historic monument", "ancient ruins", "archaeological site tourist attraction"],
            "urban": ["famous city landmark", "city square tourist attraction", "urban viewpoint"],
            "generic": ["famous landmark", "popular tourist attraction", "travel destination"],
        }
        for query in broad_queries.get(category, broad_queries["generic"]):
            if query.lower() not in {q.lower() for q in queries}:
                queries.append(query)
            add_places_from_query(query, per_query=8)
            if len(candidates) >= max_candidates:
                break

    candidates = [c for c in candidates if c.get("latitude") is not None and c.get("longitude") is not None]
    candidates.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)

    if not candidates:
        candidates = emergency_visual_fallback(summary, original_terms, max_candidates=max_candidates)
        queries.append("emergency_visual_fallback")

    if candidates:
        candidates[0]["places_search_debug"] = places_debug[-20:]

    return candidates[:max_candidates], queries

# ---------------------------------------------------------------------------
# V3 ranking override: exact-name priority + stronger photo validation
# ---------------------------------------------------------------------------
# This final override keeps the V2 robustness fallback, but improves the phase
# that converts Vision features into real Places candidates:
# - high-confidence landmarks/web entities/OCR lines become exact-name signals;
# - Places whose name/address matches those signals are strongly boosted;
# - unrelated results from broad visual queries are penalized;
# - photo comparison uses synonym groups and weighted visual categories.

NON_PLACE_ENTITY_TERMS_V3 = {
    "computer science", "engineering", "informatics", "master's degree", "data",
    "human body", "fashion", "fun", "vacation", "travel", "daytime", "summer",
    "photo shoot", "model", "barefoot", "foot", "calf", "people in nature",
}

SYNONYM_GROUPS_V3 = [
    {"university", "campus", "faculty", "college", "school", "universitat", "universidad", "facultat", "fakultatea"},
    {"building", "architecture", "facade", "office", "headquarters", "high-rise", "skyscraper"},
    {"beach", "coast", "shore", "seaside", "sea", "ocean", "sand"},
    {"desert", "dune", "sand", "aeolian", "singing sand"},
    {"mountain", "peak", "hill", "volcano", "alps", "snow"},
    {"ruins", "archaeological", "temple", "monument", "historic", "ancient", "castle", "palace"},
    {"city", "urban", "street", "square", "plaza", "metropolitan", "downtown"},
    {"cave", "caves", "rock-cut", "island"},
]

LOCATION_TYPE_HINTS_V3 = {
    "university", "campus", "faculty", "college", "school", "museum", "stadium", "park",
    "beach", "mountain", "dune", "desert", "temple", "church", "cathedral", "castle",
    "palace", "bridge", "tower", "square", "plaza", "cave", "caves", "island", "harbour",
    "harbor", "monument", "ruins", "waterfall", "lake", "river", "trail", "national park",
}


def _tokens_v3(text: str) -> set[str]:
    norm = _term_norm(text)
    toks = set()
    for t in re.split(r"\s+", norm):
        t = t.strip("'-_")
        if len(t) >= 3 and t not in STOPWORDS:
            toks.add(t)
    return toks


def _looks_like_specific_place_v3(value: str, score: float = 0.0) -> bool:
    """Heuristic: keep proper-looking names, not generic labels."""
    if not value:
        return False
    norm = _term_norm(value)
    if norm in GENERIC_TERMS or norm in NON_PLACE_ENTITY_TERMS_V3:
        return False
    toks = _tokens_v3(value)
    if not toks:
        return False
    if any(h in norm for h in LOCATION_TYPE_HINTS_V3):
        return True
    # Multi-word high-score web entities are often real names even without a type word.
    if score >= 0.45 and len(toks) >= 2:
        return True
    # Acronyms like UPC can be useful, but only when score is strong.
    if score >= 0.65 and len(value.strip()) <= 8 and value.strip().isupper():
        return True
    return False


def exact_place_signals_v3(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []

    def add(text: str, score: float, source: str) -> None:
        text = re.sub(r"\s+", " ", str(text or "").strip())
        if not text or len(text) < 3:
            return
        norm = _term_norm(text)
        if norm in { _term_norm(s["text"]) for s in signals }:
            return
        if _looks_like_specific_place_v3(text, score):
            signals.append({"text": text, "score": float(score or 0), "source": source})

    for lm in summary.get("landmarks", [])[:8]:
        add(lm.get("description", ""), float(lm.get("score", 0) or 0) + 0.35, "landmark")
    for ent in summary.get("web_entities", [])[:12]:
        add(ent.get("description", ""), float(ent.get("score", 0) or 0), "web_entity")
    for logo in summary.get("logos", [])[:5]:
        add(logo.get("description", ""), float(logo.get("score", 0) or 0) + 0.10, "logo")
    for line in (summary.get("text") or "").splitlines()[:12]:
        clean = line.strip()
        if 3 <= len(clean) <= 90:
            add(clean, 0.70, "ocr_text")

    # Prefer the strongest/specific signals first.
    signals.sort(key=lambda x: (x["score"], len(_tokens_v3(x["text"]))), reverse=True)
    return signals[:10]


def _fuzzy_name_match_v3(signal: str, candidate_text: str) -> Tuple[float, List[str]]:
    import difflib

    sig_norm = _term_norm(signal)
    cand_norm = _term_norm(candidate_text)
    if not sig_norm or not cand_norm:
        return 0.0, []

    sig_tokens = _tokens_v3(sig_norm)
    cand_tokens = _tokens_v3(cand_norm)
    matched: set[str] = set()

    if sig_norm in cand_norm or cand_norm in sig_norm:
        return 1.0, [sig_norm]

    if sig_norm.isupper() and re.search(rf"\b{re.escape(sig_norm.lower())}\b", cand_norm):
        return 0.95, [sig_norm]

    # Token overlap is robust for translations/variants like
    # "Universidad Politécnica de Cataluña" vs "Universitat Politècnica de Catalunya".
    common = sig_tokens.intersection(cand_tokens)
    matched.update(common)
    token_score = len(common) / max(1, min(len(sig_tokens), 5))

    # Fuzzy per-token matching catches small spelling/language differences.
    fuzzy_hits = 0
    for s in sig_tokens:
        if s in cand_tokens:
            continue
        if any(difflib.SequenceMatcher(None, s, c).ratio() >= 0.82 for c in cand_tokens):
            fuzzy_hits += 1
            matched.add(s)
    fuzzy_score = (len(common) + fuzzy_hits) / max(1, min(len(sig_tokens), 5))

    phrase_score = difflib.SequenceMatcher(None, sig_norm, cand_norm).ratio()
    final = max(token_score, fuzzy_score * 0.90, phrase_score * 0.75)
    return min(1.0, final), sorted(matched)


def _entity_match_score(place: Dict[str, Any], summary: Dict[str, Any]) -> float:
    """Override: much stronger boost for exact/specific Vision names."""
    candidate_text = f"{place.get('name', '')} {place.get('formatted_address', '')} {' '.join(place.get('types', []))}"
    if not candidate_text.strip():
        return 0.0

    score = 0.0
    for sig in exact_place_signals_v3(summary):
        match, _ = _fuzzy_name_match_v3(sig["text"], candidate_text)
        if match <= 0:
            continue
        source_weight = {
            "landmark": 0.55,
            "ocr_text": 0.50,
            "web_entity": 0.42,
            "logo": 0.25,
        }.get(sig["source"], 0.30)
        # A high confidence web entity/name can dominate the ranking.
        score += source_weight * min(1.0, 0.45 + match * 0.65) * min(1.0, 0.45 + sig["score"] * 0.35)

    return min(1.0, score)


def _matched_exact_signals_v3(place: Dict[str, Any], summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidate_text = f"{place.get('name', '')} {place.get('formatted_address', '')} {' '.join(place.get('types', []))}"
    out = []
    for sig in exact_place_signals_v3(summary):
        match, terms = _fuzzy_name_match_v3(sig["text"], candidate_text)
        if match >= 0.38:
            out.append({"signal": sig["text"], "source": sig["source"], "score": sig["score"], "match_score": match, "matched_terms": terms})
    out.sort(key=lambda x: x["match_score"] * (1 + x["score"]), reverse=True)
    return out[:5]


def _expand_terms_with_synonyms_v3(terms: Iterable[str]) -> set[str]:
    expanded = {_term_norm(t) for t in terms if _term_norm(t)}
    for term in list(expanded):
        for group in SYNONYM_GROUPS_V3:
            if any(g in term or term in g for g in group):
                expanded.update(group)
    return {t for t in expanded if t}


def _term_similarity(original_terms: Iterable[str], photo_terms: Iterable[str]) -> Tuple[float, List[str]]:
    """Override: visual similarity with synonym groups and less generic noise."""
    original_raw = {_term_norm(t) for t in original_terms if _term_norm(t)}
    photo_raw = {_term_norm(t) for t in photo_terms if _term_norm(t)}
    if not original_raw or not photo_raw:
        return 0.0, []

    original = _expand_terms_with_synonyms_v3(original_raw)
    photo = _expand_terms_with_synonyms_v3(photo_raw)

    matched = set(original.intersection(photo))
    for a in original:
        for b in photo:
            if len(a) >= 5 and len(b) >= 5 and (a in b or b in a):
                matched.add(a if len(a) <= len(b) else b)

    # Generic terms should not dominate, but they can contribute a little.
    strong_matches = {m for m in matched if m not in GENERIC_TERMS and m not in NON_PLACE_ENTITY_TERMS_V3}
    generic_matches = matched - strong_matches
    denom = max(4.0, min(len([t for t in original_raw if t not in GENERIC_TERMS]), 12))
    score = min(1.0, (len(strong_matches) + 0.25 * len(generic_matches)) / denom)
    return score, sorted(strong_matches)[:12] or sorted(matched)[:12]


def generate_places_queries(summary: Dict[str, Any], max_queries: int = 40) -> List[str]:
    """Override: exact names first, then visual broadening."""
    queries: List[str] = []

    # 1) Exact-name candidates first. These are the most important queries.
    for sig in exact_place_signals_v3(summary):
        name = sig["text"]
        _add_unique_query(queries, name)
        _add_unique_query(queries, f"{name} location")
        if sig["source"] in {"landmark", "web_entity", "ocr_text"}:
            _add_unique_query(queries, f"{name} point of interest")
            _add_unique_query(queries, f"{name} tourist attraction")

    # 2) Original V2 recall-heavy queries.
    for lm in summary.get("landmarks", [])[:6]:
        if lm.get("description") and float(lm.get("score", 0) or 0) >= 0.12:
            _add_unique_query(queries, lm.get("description", ""))
            _add_unique_query(queries, f"{lm.get('description', '')} tourist attraction")

    for ent in summary.get("web_entities", [])[:12]:
        desc = ent.get("description", "")
        score = float(ent.get("score", 0) or 0)
        norm = _term_norm(desc)
        if score >= 0.12 and norm and norm not in {"data", "image", "photograph"} and norm not in NON_PLACE_ENTITY_TERMS_V3:
            _add_unique_query(queries, desc)
            _add_unique_query(queries, f"{desc} location")
            if norm not in GENERIC_TERMS:
                _add_unique_query(queries, f"{desc} tourist attraction")

    for line in (summary.get("text") or "").splitlines()[:10]:
        clean = line.strip()
        if 3 <= len(clean) <= 90:
            _add_unique_query(queries, clean)
            _add_unique_query(queries, f"{clean} location")

    for label in summary.get("labels", [])[:12]:
        desc = label.get("description", "")
        norm = _term_norm(desc)
        if any(w in norm for w in LOCATION_WORDS):
            _add_unique_query(queries, desc)
            _add_unique_query(queries, f"{desc} tourist attraction")
            _add_unique_query(queries, f"{desc} travel destination")

    important_terms = collect_visual_terms(summary, include_generic=False)[:6]
    if important_terms:
        _add_unique_query(queries, " ".join(important_terms[:4]) + " location")
        _add_unique_query(queries, " ".join(important_terms[:4]) + " travel destination")
        _add_unique_query(queries, " ".join(important_terms[:3]) + " tourist attraction")

    category = _visual_category_v2(summary, important_terms)
    category_queries = {
        "university": ["university campus", "technical university campus", "computer science faculty", "university building point of interest"],
        "beach": ["famous beach", "sand beach tourist attraction", "coastal travel destination", "dune beach tourist attraction"],
        "desert": ["sand dune tourist attraction", "desert travel destination", "famous sand dunes", "singing sand tourist attraction"],
        "mountain": ["mountain viewpoint tourist attraction", "famous mountain peak", "scenic mountain travel destination"],
        "historic": ["historic ruins tourist attraction", "ancient temple tourist attraction", "famous monument", "archaeological site"],
        "urban": ["famous city square", "urban landmark", "famous city viewpoint", "downtown tourist attraction"],
        "generic": ["famous tourist attraction", "popular travel destination", "landmark point of interest"],
    }
    for q in category_queries.get(category, category_queries["generic"]):
        _add_unique_query(queries, q)

    return queries[:max_queries]


def _query_relevance_v3(query: str, place: Dict[str, Any], exact_matches: List[Dict[str, Any]]) -> float:
    base = _query_relevance(query, place)
    if exact_matches:
        return min(1.0, max(base, 0.65 + 0.25 * exact_matches[0]["match_score"]))
    return base


def _is_broad_query_v3(query: str) -> bool:
    q = _term_norm(query)
    broad_words = ["famous", "popular", "tourist attraction", "travel destination", "landmark", "city", "urban", "beach", "mountain", "desert"]
    return any(w in q for w in broad_words) and len(_tokens_v3(q)) <= 4


def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    queries = generate_places_queries(summary)
    exact_signals = exact_place_signals_v3(summary)
    has_strong_exact_signal = any(s["score"] >= 0.45 for s in exact_signals)
    seen: set[str] = set()
    candidates: List[Dict[str, Any]] = []
    places_debug: List[Dict[str, Any]] = []

    if not original_terms:
        original_terms = collect_visual_terms(summary, include_generic=True)[:12]

    def add_places_from_query(query: str, per_query: int = 6) -> None:
        try:
            places = places_text_search(query, max_results=per_query)
            places_debug.append({"query": query, "returned": len(places)})
        except Exception as e:
            places_debug.append({"query": query, "error": str(e), "returned": 0})
            return

        for place in places:
            lat, lng = _extract_location(place)
            if lat is None or lng is None:
                continue
            key = _candidate_key(place)
            if key in seen:
                continue
            seen.add(key)

            exact_matches = _matched_exact_signals_v3(place, summary)

            # When Vision found a strong specific name, avoid ranking broad-query
            # unrelated results above it unless they have strong photo evidence.
            broad_query_penalty = 0.0
            if has_strong_exact_signal and _is_broad_query_v3(query) and not exact_matches:
                broad_query_penalty = 0.18

            photos_checked: List[Dict[str, Any]] = []
            best_photo_score = 0.0
            best_matched_terms: List[str] = []

            for idx, photo in enumerate((place.get("photos") or [])[:photos_per_place], start=1):
                photo_path = _download_place_photo(photo.get("photo_reference"), key, idx)
                if not photo_path:
                    continue
                try:
                    photo_analysis = analyze_image_with_vision(photo_path)
                    photo_terms = collect_visual_terms(photo_analysis, include_generic=False)
                    if not photo_terms:
                        photo_terms = collect_visual_terms(photo_analysis, include_generic=True)
                    sim, matched = _term_similarity(original_terms, photo_terms)
                    photos_checked.append(
                        {
                            "photo_path": _relative_path(photo_path),
                            "visual_similarity_score": sim,
                            "matched_terms": matched,
                            "photo_terms_used": photo_terms[:20],
                        }
                    )
                    if sim > best_photo_score:
                        best_photo_score = sim
                        best_matched_terms = matched
                except Exception as e:
                    photos_checked.append({"photo_path": _relative_path(photo_path), "error": str(e), "visual_similarity_score": 0.0, "matched_terms": []})

            entity_score = _entity_match_score(place, summary)
            query_score = _query_relevance_v3(query, place, exact_matches)
            rating_bonus = min(0.04, max(0.0, float(place.get("rating", 0) or 0) / 125.0))

            # Exact proper-name match is now the strongest component. Photo similarity
            # is still very important, but should not destroy exact landmark matches.
            final_confidence = min(
                1.0,
                0.22 * query_score
                + 0.42 * entity_score
                + 0.28 * best_photo_score
                + rating_bonus
                + 0.10
                - broad_query_penalty,
            )

            reasons = []
            if exact_matches:
                reasons.append("The candidate name/address matches a specific place-like name detected in the media.")
            elif entity_score > 0:
                reasons.append("The place name/address partially matches entities detected in the original media.")
            if best_photo_score > 0:
                reasons.append("Google Places photos share visual terms with the original media.")
            if broad_query_penalty:
                reasons.append("Penalized because it came from a broad visual query while stronger exact-name signals were available.")
            if not reasons:
                reasons.append("Found by Google Places from generated visual/location queries.")

            candidates.append(
                {
                    "name": place.get("name", "Unknown place"),
                    "formatted_address": place.get("formatted_address", ""),
                    "latitude": lat,
                    "longitude": lng,
                    "coordinates": {"latitude": lat, "longitude": lng},
                    "place_id": place.get("place_id"),
                    "query_used": query,
                    "rating": place.get("rating"),
                    "types": place.get("types", []),
                    "matched_exact_signals": exact_matches,
                    "matched_visual_terms": best_matched_terms,
                    "photos_checked": photos_checked,
                    "scores": {
                        "query_relevance": query_score,
                        "vision_entity_match": entity_score,
                        "best_photo_visual_similarity": best_photo_score,
                        "broad_query_penalty": broad_query_penalty,
                        "final_confidence": final_confidence,
                    },
                    "final_confidence": final_confidence,
                    "reasons": reasons,
                    "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(str(place.get('name', '')) + ' ' + str(place.get('formatted_address', '')))}",
                    "source": "google_places",
                }
            )

    # First pass: exact/high-value queries. Do not stop too early; we want enough
    # candidates to rerank properly after photo checks.
    for query in queries:
        per_query = 7 if not _is_broad_query_v3(query) else 5
        add_places_from_query(query, per_query=per_query)
        if len(candidates) >= max_candidates * 4:
            break

    # Second pass only if needed.
    if len(candidates) < max_candidates:
        category = _visual_category_v2(summary, original_terms)
        broad_queries = {
            "university": ["university campus", "technical university", "computer science faculty", "campus point of interest"],
            "beach": ["beach tourist attraction", "coastal landmark", "famous beach", "sand dunes beach"],
            "desert": ["famous sand dunes", "desert tourist attraction", "sand mountain", "national park sand dunes"],
            "mountain": ["mountain tourist attraction", "famous mountain", "scenic viewpoint mountain"],
            "historic": ["historic monument", "ancient ruins", "archaeological site tourist attraction"],
            "urban": ["famous city landmark", "city square tourist attraction", "urban viewpoint"],
            "generic": ["famous landmark", "popular tourist attraction", "travel destination"],
        }
        for query in broad_queries.get(category, broad_queries["generic"]):
            if query.lower() not in {q.lower() for q in queries}:
                queries.append(query)
            add_places_from_query(query, per_query=8)
            if len(candidates) >= max_candidates * 2:
                break

    candidates = [c for c in candidates if c.get("latitude") is not None and c.get("longitude") is not None]
    candidates.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)

    if not candidates:
        candidates = emergency_visual_fallback(summary, original_terms, max_candidates=max_candidates)
        queries.append("emergency_visual_fallback")

    if candidates:
        candidates[0]["places_search_debug"] = places_debug[-30:]
        candidates[0]["exact_place_signals_used"] = exact_signals

    return candidates[:max_candidates], queries

# ---------------------------------------------------------------------------
# V4 exact Vision landmark override
# ---------------------------------------------------------------------------
# If Google Vision already returns a landmark with coordinates, that is a
# coordinate-bearing candidate and must never be thrown away just because
# Google Places Text Search returns ZERO_RESULTS/REQUEST_DENIED/etc. The previous
# fallback could replace a correct landmark (e.g. Elephanta Caves) with generic
# historic locations. These final overrides keep the exact landmark first.

_v3_find_and_rank_places = find_and_rank_places


def _direct_vision_landmark_candidates_v4(summary: Dict[str, Any], original_terms: List[str]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen: set[str] = set()

    # Web entity scores can reinforce a landmark detected directly by Vision.
    web_scores: Dict[str, float] = {}
    for ent in summary.get("web_entities", []) or []:
        desc = str(ent.get("description", "")).strip()
        if desc:
            web_scores[_term_norm(desc)] = max(web_scores.get(_term_norm(desc), 0.0), float(ent.get("score", 0) or 0))

    for lm in summary.get("landmarks", []) or []:
        name = str(lm.get("description", "")).strip()
        if not name:
            continue
        lat = lm.get("lat", lm.get("latitude"))
        lng = lm.get("lng", lm.get("longitude"))
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except Exception:
            continue

        key = f"{_term_norm(name)}|{lat_f:.6f}|{lng_f:.6f}"
        if key in seen:
            continue
        seen.add(key)

        lm_score = float(lm.get("score", 0) or 0)
        web_score = web_scores.get(_term_norm(name), 0.0)

        # Strong direct landmark coordinate evidence should outrank generic
        # fallback places. Web entity agreement boosts it further.
        final_confidence = min(0.98, 0.72 + min(lm_score, 1.0) * 0.20 + min(web_score, 1.5) * 0.06)

        matched_terms = []
        name_norm = _term_norm(name)
        for term in original_terms or []:
            t = _term_norm(term)
            if t and (t in name_norm or name_norm in t or any(part in name_norm for part in t.split() if len(part) >= 4)):
                matched_terms.append(term)
        if not matched_terms:
            matched_terms = [name.lower()]

        candidates.append(
            {
                "name": name,
                "formatted_address": "",
                "latitude": lat_f,
                "longitude": lng_f,
                "coordinates": {"latitude": lat_f, "longitude": lng_f},
                "place_id": None,
                "query_used": "vision_landmark_direct",
                "rating": None,
                "types": ["vision_landmark", "exact_place_signal"],
                "matched_exact_signals": [
                    {
                        "text": name,
                        "score": lm_score,
                        "source": "landmark",
                        "match": 1.0,
                        "matched_terms": [name.lower()],
                    }
                ],
                "matched_visual_terms": matched_terms,
                "photos_checked": [],
                "scores": {
                    "query_relevance": 1.0,
                    "vision_entity_match": min(1.0, 0.55 + min(web_score, 1.5) * 0.20),
                    "best_photo_visual_similarity": 0.0,
                    "final_confidence": final_confidence,
                    "landmark_score": lm_score,
                    "web_entity_agreement": web_score,
                    "broad_query_penalty": 0.0,
                },
                "final_confidence": final_confidence,
                "reasons": [
                    "Detected directly by Google Vision Landmark Detection with coordinates.",
                    "This exact landmark is kept even if Google Places returns no usable result.",
                ],
                "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(str(lat_f) + ',' + str(lng_f))}",
                "source": "vision_landmark_direct",
            }
        )

    candidates.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    return candidates


def _merge_candidates_keep_exact_first_v4(exact_candidates: List[Dict[str, Any]], other_candidates: List[Dict[str, Any]], max_candidates: int) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def key_for(c: Dict[str, Any]) -> str:
        name = _term_norm(str(c.get("name", "")))
        lat = c.get("latitude")
        lng = c.get("longitude")
        if lat is not None and lng is not None:
            try:
                return f"{name}|{float(lat):.5f}|{float(lng):.5f}"
            except Exception:
                pass
        return name or str(c.get("place_id") or id(c))

    # Exact Vision landmarks are always inserted first, because they are already
    # coordinate-bearing evidence from the image itself.
    for c in exact_candidates:
        k = key_for(c)
        if k not in seen:
            seen.add(k)
            merged.append(c)

    # Add Places candidates, but skip emergency fallback if we already have exact
    # image-derived candidates.
    for c in other_candidates:
        if exact_candidates and c.get("source") == "emergency_visual_fallback":
            continue
        k = key_for(c)
        if k in seen:
            continue
        # Avoid near-duplicate place names.
        c_name = _term_norm(str(c.get("name", "")))
        if any(c_name and c_name == _term_norm(str(e.get("name", ""))) for e in merged):
            continue
        seen.add(k)
        merged.append(c)

    # Keep exact landmarks first, then sort the rest by confidence.
    exact_part = [c for c in merged if c.get("source") == "vision_landmark_direct"]
    rest = [c for c in merged if c.get("source") != "vision_landmark_direct"]
    exact_part.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    rest.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    return (exact_part + rest)[:max_candidates]


def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    exact_landmarks = _direct_vision_landmark_candidates_v4(summary, original_terms)

    # Still run the Places pipeline so we can get additional candidates, photos,
    # and richer alternatives. But exact Vision landmarks are no longer replaced
    # by emergency fallback candidates.
    places_candidates, queries = _v3_find_and_rank_places(
        summary,
        original_terms,
        max_candidates=max_candidates,
        photos_per_place=photos_per_place,
    )

    candidates = _merge_candidates_keep_exact_first_v4(exact_landmarks, places_candidates, max_candidates=max_candidates)

    if exact_landmarks:
        if "vision_landmark_direct" not in queries:
            queries = ["vision_landmark_direct"] + queries
        # Put debug info on the first result without destroying existing debug.
        candidates[0].setdefault("exact_place_signals_used", exact_place_signals_v3(summary))
        candidates[0].setdefault("places_note", "Exact Google Vision landmark candidates are prioritized over broad visual fallbacks.")

    return candidates, queries

# ---------------------------------------------------------------------------
# V5 Places resolution override: robust exact web-entity resolution
# ---------------------------------------------------------------------------
# Problem fixed: if Vision detects an exact place name as a web entity
# (for example "Basílica de la Sagrada Família") but Landmark Detection does not
# return coordinates, earlier versions could still fall through to the generic
# emergency_visual_fallback. This override resolves exact web entities through
# Google Places API v1, legacy Places Text Search, and Google Geocoding before
# allowing a fallback.

PLACES_NEW_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# OCR often detects watermarks/stock-photo text. They are not real destinations.
try:
    NON_PLACE_ENTITY_TERMS_V3.update({
        "adobe", "adobe stock", "stock", "shutterstock", "getty images", "istock", "alamy",
        "viator", "guide", "tour", "tours", "closing time", "2026", "long", "sto", "abe s",
    })
except Exception:
    pass


def _convert_places_v1_to_legacy(place: Dict[str, Any], query: str = "") -> Dict[str, Any]:
    loc = place.get("location") or {}
    display = place.get("displayName") or {}
    photos = []
    for p in place.get("photos", []) or []:
        name = p.get("name")
        if name:
            # New Places photo resource name, handled by the V5 photo downloader below.
            photos.append({"photo_reference": name})
    return {
        "place_id": place.get("id") or place.get("name"),
        "name": display.get("text") or place.get("name") or query,
        "formatted_address": place.get("formattedAddress", ""),
        "geometry": {"location": {"lat": loc.get("latitude"), "lng": loc.get("longitude")}},
        "types": place.get("types", []) or [],
        "rating": place.get("rating"),
        "photos": photos,
        "_source_api": "places_v1_text_search",
    }


def _places_v1_text_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    key = _maps_key()
    if not key:
        raise RuntimeError("Missing GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY in .env")
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating,places.photos",
    }
    payload = {
        "textQuery": query,
        "maxResultCount": max(1, min(int(max_results or 5), 10)),
        "languageCode": "en",
    }
    try:
        data = _request_json(PLACES_NEW_TEXT_SEARCH_URL, method="POST", headers=headers, json=payload, timeout=30)
    except Exception:
        return []
    return [_convert_places_v1_to_legacy(p, query) for p in (data.get("places") or [])][:max_results]


def _places_legacy_text_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    key = _maps_key()
    if not key:
        raise RuntimeError("Missing GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY in .env")
    params = {"query": query, "key": key}
    data = _request_json(PLACES_TEXT_SEARCH_URL, params=params, timeout=30)
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        return []
    return data.get("results", [])[:max_results]


def _geocode_text_search(query: str, max_results: int = 3) -> List[Dict[str, Any]]:
    key = _maps_key()
    if not key:
        raise RuntimeError("Missing GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY in .env")
    params = {"address": query, "key": key}
    try:
        data = _request_json(GEOCODING_URL, params=params, timeout=30)
    except Exception:
        return []
    if data.get("status") not in ("OK", "ZERO_RESULTS"):
        return []
    out: List[Dict[str, Any]] = []
    for r in (data.get("results") or [])[:max_results]:
        loc = ((r.get("geometry") or {}).get("location") or {})
        if loc.get("lat") is None or loc.get("lng") is None:
            continue
        name = query
        # Prefer the first address component that looks like a real POI name.
        for comp in r.get("address_components", []) or []:
            types = set(comp.get("types", []) or [])
            if {"point_of_interest", "establishment", "tourist_attraction"}.intersection(types):
                name = comp.get("long_name") or name
                break
        out.append({
            "place_id": r.get("place_id"),
            "name": name,
            "formatted_address": r.get("formatted_address", ""),
            "geometry": {"location": {"lat": loc.get("lat"), "lng": loc.get("lng")}},
            "types": r.get("types", []) or ["geocode_result"],
            "rating": None,
            "photos": [],
            "_source_api": "geocoding",
        })
    return out


def places_text_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    """V5 override: try all Google location resolvers before returning empty.

    This fixes cases where legacy Places Text Search returns nothing/REQUEST_DENIED
    but Places API v1 or Geocoding can resolve the exact Vision name.
    """
    results: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def add_many(items: List[Dict[str, Any]]) -> None:
        for p in items:
            lat, lng = _extract_location(p)
            if lat is None or lng is None:
                continue
            key = p.get("place_id") or f"{_term_norm(str(p.get('name', '')))}|{lat:.5f}|{lng:.5f}"
            if key in seen:
                continue
            seen.add(key)
            results.append(p)

    # 1) New Places API text search. This is the preferred current API.
    add_many(_places_v1_text_search(query, max_results=max_results))
    if len(results) >= max_results:
        return results[:max_results]

    # 2) Legacy Text Search, useful when the old API is enabled.
    add_many(_places_legacy_text_search(query, max_results=max_results))
    if len(results) >= max_results:
        return results[:max_results]

    # 3) Geocoding fallback. Very useful for exact named places such as
    # "Basílica de la Sagrada Família" when Text Search is unavailable.
    add_many(_geocode_text_search(query, max_results=max_results))
    return results[:max_results]


_old_download_place_photo_v5 = _download_place_photo


def _download_place_photo(photo_reference: str, place_id: str, index: int) -> Optional[str]:
    """V5 override: support both legacy photo_reference and Places v1 photo names."""
    key = _maps_key()
    if not key or not photo_reference:
        return None
    try:
        if str(photo_reference).startswith("places/"):
            url = f"https://places.googleapis.com/v1/{photo_reference}/media"
            params = {"maxHeightPx": 800, "maxWidthPx": 800, "key": key}
            response = requests.get(url, params=params, timeout=30, allow_redirects=True)
            if response.status_code != 200 or not response.content:
                return None
            out_path = PLACE_PHOTO_DIR / f"{_safe_name(place_id)}_{index}.jpg"
            out_path.write_bytes(response.content)
            return str(out_path)
    except Exception:
        return None
    return _old_download_place_photo_v5(photo_reference, place_id, index)

def _check_place_photos(place: Dict[str, Any], original_terms: List[str], photos_per_place: int) -> Tuple[float, List[str], List[Dict[str, Any]]]:
    """Download and analyze Places photos, then compare their Vision terms with the original image/video terms."""
    photos_checked: List[Dict[str, Any]] = []
    best_score = 0.0
    best_matched: List[str] = []

    try:
        photos = place.get("photos") or []
    except Exception:
        photos = []

    if not photos or photos_per_place <= 0:
        return 0.0, [], []

    place_id = str(place.get("place_id") or place.get("id") or place.get("name") or "place")

    for idx, photo in enumerate(photos[: max(0, int(photos_per_place))], start=1):
        photo_reference = None
        if isinstance(photo, dict):
            photo_reference = photo.get("photo_reference") or photo.get("name")
        elif isinstance(photo, str):
            photo_reference = photo

        if not photo_reference:
            photos_checked.append({"photo_path": None, "error": "Missing photo_reference/name in Places photo object.", "visual_similarity_score": 0.0, "matched_terms": []})
            continue

        try:
            photo_path = _download_place_photo(str(photo_reference), place_id, idx)
            if not photo_path:
                photos_checked.append({"photo_path": None, "error": "Could not download Places photo.", "visual_similarity_score": 0.0, "matched_terms": []})
                continue

            photo_analysis = analyze_image_with_vision(photo_path)
            photo_terms = collect_visual_terms(photo_analysis, include_generic=False)
            score, matched = _term_similarity(original_terms, photo_terms)

            photos_checked.append({"photo_path": _relative_path(photo_path), "visual_similarity_score": score, "matched_terms": matched})

            if score > best_score:
                best_score = score
                best_matched = matched
        except Exception as e:
            photos_checked.append({"photo_path": None, "error": str(e), "visual_similarity_score": 0.0, "matched_terms": []})

    return best_score, best_matched, photos_checked


_v4_find_and_rank_places = find_and_rank_places


def _direct_exact_signal_candidates_v5(summary: Dict[str, Any], original_terms: List[str], max_candidates: int, photos_per_place: int) -> List[Dict[str, Any]]:
    """Resolve high-confidence Vision web-entity/OCR names directly.

    Landmark coordinates are already handled by V4. This function handles the
    common case where Vision has no landmark object but has a very strong web
    entity, e.g. Sagrada Família.
    """
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    signals = exact_place_signals_v3(summary)

    for sig in signals[:8]:
        text = sig.get("text", "")
        source = sig.get("source", "")
        score = float(sig.get("score", 0) or 0)
        norm = _term_norm(text)
        if not norm or norm in NON_PLACE_ENTITY_TERMS_V3:
            continue
        # Do not over-trust weak OCR. Web entities above 0.55 are strong exact signals.
        if source == "ocr_text" and score < 0.85:
            continue
        if source == "web_entity" and score < 0.55:
            continue

        # Try the exact name first, then a light disambiguation if the signal/name hints Barcelona.
        queries = [text]
        if "sagrada" in norm or "gaudí" in norm or "gaudi" in norm or "batll" in norm or "güell" in norm or "guell" in norm:
            queries.append(f"{text} Barcelona")
        if "familia" in norm and "sagrada" in norm:
            queries.append("Sagrada Familia Barcelona Spain")
        if "basílica" in norm or "basilica" in norm:
            queries.append(text.replace("Basílica de la ", ""))

        found: List[Dict[str, Any]] = []
        for q in queries:
            found = places_text_search(q, max_results=3)
            if found:
                break
        for place in found[:2]:
            lat, lng = _extract_location(place)
            if lat is None or lng is None:
                continue
            candidate_text = f"{place.get('name', '')} {place.get('formatted_address', '')} {' '.join(place.get('types', []))}"
            match_score, matched_terms = _fuzzy_name_match_v3(text, candidate_text)
            # If Geocoding returns a generic administrative result with no name match,
            # do not promote it as an exact candidate.
            if match_score < 0.25 and source != "landmark":
                continue
            key = place.get("place_id") or f"{_term_norm(str(place.get('name','')))}|{lat:.5f}|{lng:.5f}"
            if key in seen:
                continue
            seen.add(key)

            photo_score, matched_visual, photos_checked = _check_place_photos(place, original_terms, photos_per_place)
            entity_score = max(_entity_match_score(place, summary), min(1.0, 0.45 + score * 0.25 + match_score * 0.35))
            final_confidence = min(0.99, 0.56 + entity_score * 0.28 + photo_score * 0.16)

            reasons = [
                f"Resolved exact Vision {source.replace('_', ' ')} signal: {text}.",
                "This exact detected name is prioritized over broad visual fallbacks.",
            ]
            if photos_checked:
                reasons.append("Compared candidate Places photos with the original image using Vision visual terms.")

            out.append({
                "name": place.get("name") or text,
                "formatted_address": place.get("formatted_address", ""),
                "latitude": lat,
                "longitude": lng,
                "coordinates": {"latitude": lat, "longitude": lng},
                "place_id": place.get("place_id"),
                "query_used": text,
                "rating": place.get("rating"),
                "types": place.get("types", []) or ["exact_place_signal"],
                "matched_exact_signals": [{
                    "text": text,
                    "score": score,
                    "source": source,
                    "match": match_score,
                    "matched_terms": matched_terms,
                }],
                "matched_visual_terms": matched_visual,
                "photos_checked": photos_checked,
                "scores": {
                    "query_relevance": 1.0,
                    "vision_entity_match": entity_score,
                    "best_photo_visual_similarity": photo_score,
                    "final_confidence": final_confidence,
                    "exact_signal_score": score,
                    "broad_query_penalty": 0.0,
                },
                "final_confidence": final_confidence,
                "reasons": reasons,
                "maps_url": f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(str(place.get('name', text)) + ' ' + str(place.get('formatted_address', '')))}",
                "source": f"vision_{source}_resolved",
            })

    out.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    return out[:max_candidates]


def _merge_exact_signal_candidates_v5(exact: List[Dict[str, Any]], others: List[Dict[str, Any]], max_candidates: int) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def key_for(c: Dict[str, Any]) -> str:
        name = _term_norm(str(c.get("name", "")))
        pid = c.get("place_id")
        lat = c.get("latitude")
        lng = c.get("longitude")
        if pid:
            return str(pid)
        if lat is not None and lng is not None:
            try:
                return f"{name}|{float(lat):.5f}|{float(lng):.5f}"
            except Exception:
                pass
        return name

    for c in exact:
        k = key_for(c)
        if k and k not in seen:
            seen.add(k)
            merged.append(c)

    for c in others:
        if exact and c.get("source") == "emergency_visual_fallback":
            # Never show Colosseum/Acropolis/etc. if we have resolved an exact
            # Vision place name such as Sagrada Família.
            continue
        k = key_for(c)
        if not k or k in seen:
            continue
        seen.add(k)
        merged.append(c)

    exact_sources = {"vision_landmark_direct", "vision_web_entity_resolved", "vision_ocr_text_resolved"}
    exact_part = [c for c in merged if c.get("source") in exact_sources]
    rest = [c for c in merged if c.get("source") not in exact_sources]
    exact_part.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    rest.sort(key=lambda c: c.get("scores", {}).get("final_confidence", 0), reverse=True)
    return (exact_part + rest)[:max_candidates]


def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    resolved_exact = _direct_exact_signal_candidates_v5(summary, original_terms, max_candidates=max_candidates, photos_per_place=photos_per_place)

    # Run the existing V4 pipeline as well for alternatives and broad candidates.
    other_candidates, queries = _v4_find_and_rank_places(
        summary,
        original_terms,
        max_candidates=max_candidates,
        photos_per_place=photos_per_place,
    )

    candidates = _merge_exact_signal_candidates_v5(resolved_exact, other_candidates, max_candidates=max_candidates)
    if resolved_exact:
        queries = ["exact_web_entity_or_ocr_resolution"] + queries
        candidates[0].setdefault("places_note", "High-confidence exact Vision web entities/OCR names are resolved with Places v1 + legacy Places + Geocoding and prioritized over visual fallbacks.")
        candidates[0].setdefault("exact_place_signals_used", exact_place_signals_v3(summary))
    return candidates, queries

# ---------------------------------------------------------------------------
# V7 speed override
# ---------------------------------------------------------------------------
# The v6 patch can be slow because it resolves many broad queries and can run
# Vision again on several Google Places photos. This override keeps the same
# output format but makes the pipeline return quickly:
# - cap Places-photo verification to 0/1 photos by default;
# - use short network timeouts;
# - if an exact Vision signal is resolved (e.g. Sagrada Família), do not run the
#   expensive broad fallback pipeline;
# - if exact resolution fails, run the broad pipeline in a cheaper mode.

import os as _os_v7
import time as _time_v7

_FAST_MODE_V7 = (_os_v7.getenv("LOCATION_FAST_MODE", "1").strip().lower() not in {"0", "false", "no"})
_MAX_PHOTOS_FAST_V7 = int(_os_v7.getenv("MAX_PHOTOS_TO_VERIFY", "1" if _FAST_MODE_V7 else "2") or 1)
_SKIP_PHOTOS_FOR_EXACT_V7 = (_os_v7.getenv("SKIP_PHOTO_CHECK_FOR_EXACT", "1").strip().lower() not in {"0", "false", "no"})
_MAX_BROAD_QUERIES_V7 = int(_os_v7.getenv("MAX_BROAD_QUERIES", "12" if _FAST_MODE_V7 else "30") or 12)

_old_request_json_v7 = _request_json

def _request_json(url: str, *, method: str = "GET", timeout: int = 30, **kwargs) -> Dict[str, Any]:
    """V7 override: shorter timeouts so the browser does not wait for minutes."""
    if _FAST_MODE_V7:
        timeout = min(int(timeout or 12), int(_os_v7.getenv("API_TIMEOUT_SECONDS", "12") or 12))
    return _old_request_json_v7(url, method=method, timeout=timeout, **kwargs)

_old_download_place_photo_v7 = _download_place_photo

def _download_place_photo(photo_reference: str, place_id: str, index: int) -> Optional[str]:
    """V7 override: shorter photo download timeout for Places v1 photos."""
    key = _maps_key()
    if not key or not photo_reference:
        return None
    try:
        if str(photo_reference).startswith("places/"):
            url = f"https://places.googleapis.com/v1/{photo_reference}/media"
            params = {"maxHeightPx": 600, "maxWidthPx": 600, "key": key}
            response = requests.get(url, params=params, timeout=int(_os_v7.getenv("PHOTO_TIMEOUT_SECONDS", "8") or 8), allow_redirects=True)
            if response.status_code != 200 or not response.content:
                return None
            out_path = PLACE_PHOTO_DIR / f"{_safe_name(place_id)}_{index}.jpg"
            out_path.write_bytes(response.content)
            return str(out_path)
    except Exception:
        return None
    return _old_download_place_photo_v7(photo_reference, place_id, index)

_old_check_place_photos_v7 = _check_place_photos

def _check_place_photos(place: Dict[str, Any], original_terms: List[str], photos_per_place: int) -> Tuple[float, List[str], List[Dict[str, Any]]]:
    """V7 override: cap photo verification and allow disabling it.

    Photo verification is useful, but it is the slowest part because every photo
    requires download + another Vision API call. Exact Vision-name matches are
    already strong, so by default we skip photo verification for exact candidates.
    """
    if _os_v7.getenv("VERIFY_PLACE_PHOTOS", "1").strip().lower() in {"0", "false", "no"}:
        return 0.0, [], [{"skipped": True, "reason": "VERIFY_PLACE_PHOTOS=0"}]
    capped = max(0, min(int(photos_per_place or 0), _MAX_PHOTOS_FAST_V7))
    if capped <= 0:
        return 0.0, [], []
    start = _time_v7.time()
    result = _old_check_place_photos_v7(place, original_terms, capped)
    # Add lightweight timing metadata for debugging.
    try:
        score, matched, checked = result
        if checked:
            checked[0].setdefault("photo_check_seconds", round(_time_v7.time() - start, 2))
        return score, matched, checked
    except Exception:
        return result

_old_direct_exact_signal_candidates_v7 = _direct_exact_signal_candidates_v5

def _direct_exact_signal_candidates_v5(summary: Dict[str, Any], original_terms: List[str], max_candidates: int, photos_per_place: int) -> List[Dict[str, Any]]:
    # Skip Places-photo verification for exact detected names by default. This
    # makes cases like Sagrada Família return quickly while still using
    # Places/Geocoding for real coordinates.
    effective_photos = 0 if (_FAST_MODE_V7 and _SKIP_PHOTOS_FOR_EXACT_V7) else min(photos_per_place, _MAX_PHOTOS_FAST_V7)
    out = _old_direct_exact_signal_candidates_v7(summary, original_terms, max_candidates, effective_photos)
    for c in out:
        c.setdefault("speed_mode", {})
        c["speed_mode"].update({
            "exact_signal_short_circuit": True,
            "photo_check_skipped_for_exact": effective_photos == 0,
        })
        if effective_photos == 0:
            c.setdefault("photos_checked", [])
            c["photos_checked"].append({"skipped": True, "reason": "Exact Vision place name resolved; photo verification skipped for speed. Set SKIP_PHOTO_CHECK_FOR_EXACT=0 to enable."})
    return out

_old_generate_places_queries_v7 = generate_places_queries

def generate_places_queries(summary: Dict[str, Any], max_queries: int = 30) -> List[str]:
    queries = _old_generate_places_queries_v7(summary, max_queries=max_queries)
    if _FAST_MODE_V7:
        # Keep exact/name-like queries first and avoid spending time on many broad
        # generic queries after strong web entities have already been tried.
        filtered = []
        bad = {"adobe stock", "adobe", "viator", "2026", "guide", "long"}
        for q in queries:
            qn = _term_norm(q)
            if not qn or qn in bad:
                continue
            filtered.append(q)
            if len(filtered) >= _MAX_BROAD_QUERIES_V7:
                break
        return filtered
    return queries

_old_find_and_rank_places_v7 = find_and_rank_places

def find_and_rank_places(
    summary: Dict[str, Any],
    original_terms: List[str],
    max_candidates: int = 5,
    photos_per_place: int = 2,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """V7 override: fast path for exact detected places.

    If Vision detects and resolves an exact place name, return it immediately
    plus a few cheap exact alternatives. Do not run the emergency historic
    fallback unless no exact candidates were resolved.
    """
    effective_photos = min(int(photos_per_place or 0), _MAX_PHOTOS_FAST_V7)
    start = _time_v7.time()

    resolved_exact = _direct_exact_signal_candidates_v5(
        summary,
        original_terms,
        max_candidates=max_candidates,
        photos_per_place=effective_photos,
    )

    if resolved_exact and _FAST_MODE_V7:
        # When the top exact candidate is strong, return now. This prevents the
        # UI from waiting for broad fallback checks and prevents Colosseum/etc.
        # from replacing an obvious exact web entity.
        top = resolved_exact[0]
        top_conf = float(top.get("scores", {}).get("final_confidence", top.get("final_confidence", 0)) or 0)
        if top_conf >= float(_os_v7.getenv("EXACT_FAST_RETURN_CONFIDENCE", "0.62") or 0.62):
            for c in resolved_exact:
                c.setdefault("speed_mode", {})
                c["speed_mode"].update({"returned_without_broad_fallback": True, "analysis_seconds": round(_time_v7.time() - start, 2)})
            return resolved_exact[:max_candidates], ["fast_exact_signal_resolution"] + [s.get("text", "") for s in exact_place_signals_v3(summary)[:8]]

    # No strong exact match: fall back to the previous logic, but with capped
    # photos and fewer broad queries.
    candidates, queries = _old_find_and_rank_places_v7(
        summary,
        original_terms,
        max_candidates=max_candidates,
        photos_per_place=effective_photos,
    )
    for c in candidates:
        c.setdefault("speed_mode", {})
        c["speed_mode"].setdefault("analysis_seconds", round(_time_v7.time() - start, 2))
    return candidates, queries
