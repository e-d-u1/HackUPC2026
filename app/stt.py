from google import genai
import os
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def transcribe_audio(audio_file) -> str:
    """Transcribes audio using Gemini 1.5 Flash."""
    audio_bytes = audio_file.read()

    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=[
            {
                "inline_data": {
                    "mime_type": "audio/webm",
                    "data": audio_bytes
                }
            },
            "Transcribe exactly what is said in this audio. Return only the transcription, nothing else."
        ]
    )

    return response.text.strip()