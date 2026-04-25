from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from app.schemas import PlaceInput
from app.stt import transcribe_audio
from app.llm import get_travel_recommendations
import json

app = FastAPI(title="Travel Recommendations API")

@app.get("/")
def health_check():
    return {"status": "ok"}

# Endpoint 1: solo JSON (sin audio)
@app.post("/api/recommendations")
def recommendations(place: PlaceInput):
    try:
        result = get_travel_recommendations(place.model_dump())
        return {"recommendations": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint 2: audio + JSON del lugar
@app.post("/api/recommendations/audio")
async def recommendations_with_audio(
    audio: UploadFile = File(...),
    place_data: str = Form(...)        # JSON del lugar como string
):
    try:
        place_dict = json.loads(place_data)
        transcript = transcribe_audio(audio.file)
        result = get_travel_recommendations(place_dict, user_context=transcript)
        return {
            "transcript": transcript,
            "recommendations": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))