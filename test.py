# test_api.py
import requests
import json

place_data = {
    "name": "Mirador Sarrià",
    "formatted_address": "Carretera de Sarrià a Vallvidrera, 115, Barcelona",
    "latitude": 41.4056071,
    "longitude": 2.11,
    "matched_visual_terms": ["city", "metropolitan area", "urban area"],
    "types": ["observation_deck", "tourist_attraction"],
    "rating": 4.5,
    "reasons": ["Algunas fotos del lugar comparten pistas visuales con la imagen original."]
}

with open("audio_prueba.mp3", "rb") as audio_file:
    response = requests.post(
        "http://localhost:8000/api/recommendations/audio",
        files={"audio": ("audio_prueba.mp3", audio_file, "audio/mpeg")},
        data={"place_data": json.dumps(place_data)}
    )

print(response.json())