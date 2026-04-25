import requests
import os
from dotenv import load_dotenv

load_dotenv()

class HotelSearcher:
    def __init__(self, api_key):
        self.api_key = api_key
        # URL de la API de hoteles de Skyscanner
        self.base_url = "https://partners.api.skyscanner.net/apiservices/v3/hotels/live/search/create"
        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

    def get_hotel_prices(self, destination_name):
        """Busca el hotel más barato (Simulado para el MVP)"""
        try:
            # Aquí iría la llamada real, por ahora devolvemos un estimado
            # basado en el nombre para que parezca dinámico
            precios_simulados = {
                "Paris": "120€", "Tokio": "85€", "Londres": "150€", "Madrid": "70€"
            }
            return precios_simulados.get(destination_name, "95€") + "/noche"
        except Exception:
            return "No disponible"