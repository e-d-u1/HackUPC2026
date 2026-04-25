import google.generativeai as generativeai
import os
from dotenv import load_dotenv

load_dotenv()

generativeai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def get_travel_recommendations(place_data: dict, user_context: str = "") -> str:
    model = generativeai.GenerativeModel("gemini-2.5-flash-lite")

    user_section = f"\n- Lo que el usuario dijo: {user_context}" if user_context else ""

    prompt = f"""
        Un usuario está interesado en un lugar con estas características:

        - Nombre: {place_data.get('name', 'N/A')}
        - Dirección: {place_data.get('formatted_address', 'N/A')}
        - Tipos: {', '.join(place_data.get('types', []))}
        - Términos visuales: {', '.join(place_data.get('matched_visual_terms', []))}
        - Rating: {place_data.get('rating', 'N/A')}{user_section}

        Quiero que recomiendes EXACTAMENTE 5 ciudades del mundo donde esta persona podría disfrutar viajar.

        INSTRUCCIONES IMPORTANTES:
        - Solo devuelve las 5 ciudades (no más, no menos).
        - No incluyas introducción ni conclusión.
        - Para cada ciudad, da 3 o 4 razones.
        - Cada razón debe ser una frase corta (máximo 1 línea).
        - Las razones deben explicar por qué le interesaría a esta persona basado en el lugar original.
        - Responde en español.

        FORMATO:

        Ciudad: [Nombre]
        - Razón 1
        - Razón 2
        - Razón 3
        - Razón 4 (opcional)
        """
    response = model.generate_content(prompt)
    return response.text