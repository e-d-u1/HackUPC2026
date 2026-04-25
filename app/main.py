from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from app.schemas import PlaceInput
from app.stt import transcribe_audio
from app.llm import get_travel_recommendations
import json

app = FastAPI(title="Travel Recommendations API")

@app.get("/")
def health_check():
    return {"status": "ok"}

# FASE 1: Solo place_data → 5 ciudades
@app.post("/api/recommendations")
def recommendations(place: PlaceInput):
    try:
        result = get_travel_recommendations(place.model_dump())
        return {"recommendations": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# FASE 2: place_data + audio → 5 ciudades refinadas
@app.post("/api/recommendations/audio")
async def recommendations_with_audio(
    audio: UploadFile = File(...),
    place_data: str = Form(...)
):
    try:
        place_dict = json.loads(place_data)
        place = PlaceInput(**place_dict)          # ← validación igual que fase 1

        transcript = transcribe_audio(audio.file)
        result = get_travel_recommendations(
            place.model_dump(),
            user_context=transcript
        )
        return {
            "transcript": transcript,
            "recommendations": result
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="place_data no es JSON válido")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))