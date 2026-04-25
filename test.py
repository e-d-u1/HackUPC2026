# test_api.py
import requests, json

place_data = {
    "name": "Mirador Sarrià",
    "formatted_address": "Carretera de Sarrià a Vallvidrera, 115, Barcelona",
    "latitude": 41.4056071,
    "longitude": 2.11,
    "matched_visual_terms": ["city", "metropolitan area", "urban area"],
    "types": ["observation_deck", "tourist_attraction"],
    "rating": 4.5,
    "reasons": []
}

# --- FASE 1 ---
print("=== FASE 1: Solo lugar ===")
r1 = requests.post(
    "http://localhost:8000/api/recommendations",
    json=place_data
)
print(r1.json()["recommendations"])

# --- FASE 2 ---
print("\n=== FASE 2: Con audio ===")
with open("audioIshak.mp3", "rb") as f:
    r2 = requests.post(
        "http://localhost:8000/api/recommendations/audio",
        files={"audio": ("audioIshak.mp3", f, "audio/mpeg")},
        data={"place_data": json.dumps(place_data)}
    )
print(r2.json()["transcript"])
print(r2.json()["recommendations"])