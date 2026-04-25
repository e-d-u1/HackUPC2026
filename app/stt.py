from elevenlabs.client import ElevenLabs
import os
from dotenv import load_dotenv

load_dotenv()

client = ElevenLabs(api_key=os.getenv("ELEVENLABS_API_KEY"))

def transcribe_audio(audio_file, language_code: str = "es") -> str:
    result = client.speech_to_text.convert(
        file=audio_file,
        model_id="scribe_v1",
        language_code=language_code,
    )
    return result.text