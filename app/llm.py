from google import genai
from google.genai import types
import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def analyze_image_for_locations(image_bytes: bytes, mime_type: str = "image/jpeg") -> dict:
    """
    Sends an image to Gemini multimodal and asks it to identify
    possible travel destinations shown in the image.
    Returns a dict with a 'locations' list.
    """

    prompt = """Analiza esta imagen y determina qué lugar(es) del mundo podrían ser.

INSTRUCCIONES:
- Identifica entre 1 y 5 posibles ubicaciones que coincidan con lo que se ve en la imagen.
- Para cada ubicación, proporciona: ciudad, país, coordenadas (latitud, longitud), nivel de confianza (0.0 a 1.0), tipo de clima, tipo de paisaje, y una breve descripción.
- Ordena por confianza de mayor a menor.
- Si reconoces un monumento o lugar exacto, la confianza debe ser alta (>0.8).
- Si es un paisaje genérico, proporciona varias ciudades que coincidan con ese tipo de paisaje.

FORMATO DE RESPUESTA (JSON estricto, sin markdown):
{
  "locations": [
    {
      "city": "Nombre de la ciudad",
      "country": "País",
      "latitude": 0.0,
      "longitude": 0.0,
      "confidence": 0.0,
      "climate": "Tipo de clima (ej: Tropical, Mediterráneo, Continental...)",
      "landscape": "Tipo de paisaje (ej: Playa, Montaña, Urbano, Histórico...)",
      "description": "Breve descripción de por qué esta ubicación coincide con la imagen"
    }
  ]
}

Responde SOLO con el JSON, sin ningún texto adicional ni bloques de código."""

    image_part = types.Part.from_bytes(data=image_bytes, mime_type=mime_type)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt, image_part],
    )
    text = response.text.strip()

    # Clean markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        # Attempt to extract JSON from the response
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            result = json.loads(match.group())
        else:
            result = {"locations": []}

    # Ensure structure
    if "locations" not in result:
        result = {"locations": []}

    return result


def refine_locations_with_voice(current_locations: list, transcript: str) -> dict:
    """
    Takes the current AI-detected locations and the user's voice transcript,
    then asks Gemini to correct/refine the list based on what the user said.
    Returns a dict with a 'locations' list.
    """

    locations_json = json.dumps(current_locations, ensure_ascii=False, indent=2)

    prompt = f"""Un sistema de IA ha analizado una imagen y ha detectado estos posibles destinos de viaje:

{locations_json}

El usuario ha grabado un audio diciendo lo siguiente:
"{transcript}"

INSTRUCCIONES:
- Analiza lo que dijo el usuario.
- Si el usuario corrige alguna ubicación (ej: "No es París, es Lyon"), reemplázala.
- Si el usuario confirma las ubicaciones, devuélvelas tal cual.
- Si el usuario añade nuevas ubicaciones, añádelas a la lista.
- Si el usuario elimina alguna, quítala.
- Mantén entre 1 y 5 ubicaciones en la lista final.
- Para nuevas ubicaciones, genera coordenadas, clima, paisaje y descripción.
- Ajusta los niveles de confianza: las confirmadas por el usuario deben tener confianza alta (>0.9).

FORMATO DE RESPUESTA (JSON estricto, sin markdown):
{{
  "locations": [
    {{
      "city": "Nombre de la ciudad",
      "country": "País",
      "latitude": 0.0,
      "longitude": 0.0,
      "confidence": 0.0,
      "climate": "Tipo de clima",
      "landscape": "Tipo de paisaje",
      "description": "Breve descripción"
    }}
  ]
}}

Responde SOLO con el JSON, sin ningún texto adicional ni bloques de código."""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    text = response.text.strip()

    # Clean markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            result = json.loads(match.group())
        else:
            # Fallback: return original locations unchanged
            result = {"locations": current_locations}

    if "locations" not in result:
        result = {"locations": current_locations}

    return result