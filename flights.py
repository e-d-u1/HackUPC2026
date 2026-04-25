import requests
import time
import math
import os
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from dotenv import load_dotenv

load_dotenv()

class SkyscannerOptimizer:
    def __init__(self, api_key, market="ES", locale="es-ES", currency="EUR"):
        self.api_key = api_key
        self.base_url = "https://partners.api.skyscanner.net/apiservices/v3"
        self.headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.config = {
            "market": market,
            "locale": locale,
            "currency": currency
        }
        self.geolocator = Nominatim(user_agent="skyscanner_optimizer")

    def haversine(self, lat1, lon1, lat2, lon2):
        """Calcula la distancia en km entre dos puntos."""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
            math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c

    def get_city_entity(self, city_name):
        """Obtiene el Entity ID de Skyscanner para una ciudad."""
        url = f"{self.base_url}/autosuggest/flights"
        payload = {
            "query": {
                "searchTerm": city_name,
                "locale": self.config["locale"],
                "market": self.config["market"]
            }
        }
        try:
            response = requests.post(url, json=payload, headers=self.headers)
        except requests.exceptions.ConnectionError:
            return None

        if response.status_code == 200:
            data = response.json()
            results = data.get("content", {}).get("results", {})
            places_data = results.get("places", data.get("places", []))
            items = places_data.values() if isinstance(places_data, dict) else places_data
            if items:
                for p in items:
                    eid = p.get("entityId")
                    if eid: return eid
        return None

    def get_nearest_airports_fallback(self, city_name, radius_km=150):
        """Busca aeropuertos cercanos (radio 150km) si la ciudad no tiene ID propio."""
        location = self.geolocator.geocode(city_name)
        if not location:
            return None

        url = f"{self.base_url}/geo/hierarchy/flights/nearest"
        payload = {
            "locator": {
                "coordinates": {
                    "latitude": location.latitude,
                    "longitude": location.longitude
                }
            },
            "locale": self.config["locale"]
        }
        
        response = requests.post(url, json=payload, headers=self.headers)
        if response.status_code == 200:
            places = response.json().get("places", {})
            # Skyscanner devuelve una jerarquía. Filtramos los que sean de tipo 'AIRPORT'
            # y calculamos la distancia real para validar el radio.
            valid_airports = []
            for p_id, p_info in places.items():
                if p_info.get("type") == "PLACE_TYPE_AIRPORT":
                    coords = p_info.get("coordinates", {})
                    dist = self.haversine(location.latitude, location.longitude, 
                                          coords.get("latitude"), coords.get("longitude"))
                    if dist <= radius_km:
                        valid_airports.append({
                            "entityId": p_id,
                            "name": p_info.get("name"),
                            "distance": dist
                        })
            return sorted(valid_airports, key=lambda x: x['distance'])
        return None

    def search_cheapest_flight(self, origin_id, dest_id, date):
        """Crea una sesión de búsqueda y hace polling para obtener el mejor precio."""
        search_url = f"{self.base_url}/flights/live/search/create"
        query = {
            "query": {
                "market": self.config["market"],
                "locale": self.config["locale"],
                "currency": self.config["currency"],
                "queryLegs": [{
                    "originPlaceId": {"entityId": origin_id},
                    "destinationPlaceId": {"entityId": dest_id},
                    "date": {"year": int(date[:4]), "month": int(date[5:7]), "day": int(date[8:10])}
                }],
                "adults": 1,
                "cabinClass": "CABIN_CLASS_ECONOMY"
            }
        }

        res = requests.post(search_url, json=query, headers=self.headers)
        if res.status_code != 200:
            return None

        token = res.json().get("sessionToken")
        
        # Polling (reintento simplificado)
        poll_url = f"{self.base_url}/flights/live/search/poll/{token}"
        for _ in range(5): # Máximo 5 intentos
            time.sleep(2)
            poll_res = requests.get(poll_url, headers=self.headers)
            data = poll_res.json()
            
            if data.get("status") == "RESULT_STATUS_COMPLETE":
                # Lógica para extraer el precio más bajo
                itineraries = data.get("content", {}).get("results", {}).get("itineraries", {})
                if not itineraries: return "Sin vuelos disponibles"
                
                prices = []
                for _, itin in itineraries.items():
                    for option in itin.get("pricingOptions", []):
                        prices.append(float(option["price"]["amount"])) 
                
                return min(prices) if prices else "Error en precios"
        
        return "Tiempo de espera agotado"

    def search_indicative_cheapest(self, origin_id, dest_id, year, month):
        """Busca el precio más barato en un mes completo usando datos de caché (Indicative)."""
        url = f"{self.base_url}/flights/indicative/search"
        payload = {
            "query": {
                "market": self.config["market"],
                "locale": self.config["locale"],
                "currency": self.config["currency"],
                "queryLegs": [{
                    "originPlace": {"queryPlace": {"entityId": origin_id}},
                    "destinationPlace": {"queryPlace": {"entityId": dest_id}},
                    "fixedDate": {"year": int(year), "month": int(month), "day": 1}
                }]
            }
        }
        res = requests.post(url, json=payload, headers=self.headers)
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("content", {}).get("results", {}).get("quotes", {})
            
            if not quotes:
                return "Sin datos para este mes"
            
            prices = []
            for q in (quotes.values() if isinstance(quotes, dict) else quotes):
                amt = q.get("minPrice", {}).get("amount")
                if amt: prices.append(float(amt))
            
            return min(prices) if prices else "Sin precios"
        return f"Error API ({res.status_code}: {res.text[:50]})"
    
    def _get_best_price(self, origin_id, dest_id, date):
        """Determina qué búsqueda realizar según el formato: YYYY, YYYY-MM o YYYY-MM-DD."""
        if len(date) == 4:  # Búsqueda de los próximos 12 meses (Rolling Year desde hoy)
            prices = []
            now = datetime.now()
            for i in range(12):
                # Calculamos el mes y año para cada uno de los próximos 12 meses
                month = (now.month + i - 1) % 12 + 1
                year = now.year + (now.month + i - 1) // 12
                p = self.search_indicative_cheapest(origin_id, dest_id, year, month)
                if isinstance(p, (int, float)): prices.append(p)
            return min(prices) if prices else "Sin vuelos en los próximos 12 meses"
        
        elif len(date) == 7:  # Búsqueda mensual (YYYY-MM)
            year, month = date.split('-')
            return self.search_indicative_cheapest(origin_id, dest_id, year, month)
        
        else:  # Búsqueda de día exacto (YYYY-MM-DD)
            return self.search_cheapest_flight(origin_id, dest_id, date)

    def optimize_route(self, cities, date):
        origin = cities[0]
        destinations = cities[1:]
        
        origin_id = self.get_city_entity(origin)
        if not origin_id:
            print(f" Origen {origin} no reconocido. Buscando aeropuertos cercanos...")
            nearby = self.get_nearest_airports_fallback(origin)
            if nearby:
                origin_id = nearby[0]['entityId']
                print(f"✅ Usando aeropuerto cercano: {nearby[0]['name']}")
            else:
                return "Error: No se pudo localizar el origen ni aeropuertos cercanos."

        results = {}
        for dest in destinations:
            print(f"🔎 Buscando para: {dest}...")
            dest_id = self.get_city_entity(dest)
            
            if not dest_id:
                print(f"   ⚠️ {dest} sin conexión directa. Aplicando fallback...")
                nearby = self.get_nearest_airports_fallback(dest)
                if nearby:
                    # Probamos con el más cercano disponible
                    best_p = "N/A"
                    for airport in nearby[:2]: # Probamos los 2 más cercanos
                        price = self._get_best_price(origin_id, airport['entityId'], date)
                        if isinstance(price, float):
                            best_p = f"{price} {self.config['currency']} (vía {airport['name']})"
                            break
                    results[dest] = best_p
                else:
                    results[dest] = "No se encontraron aeropuertos en el radio."
            else:
                price = self._get_best_price(origin_id, dest_id, date)
                results[dest] = f"{price} {self.config['currency']}"
        
        return results

# --- EJEMPLO DE USO ---
if __name__ == "__main__":
    MI_API_KEY = os.getenv("SKYSCANNER_API_KEY")
    if not MI_API_KEY:
        print("❌ Error: No se encontró SKYSCANNER_API_KEY en el archivo .env")
        exit()

    optimizer = SkyscannerOptimizer(MI_API_KEY)

    # --- LÓGICA PARA FECHAS DINÁMICAS ---
    hoy = datetime.now()
    proximo_mes = (hoy.replace(day=28) + timedelta(days=4)).strftime("%Y-%m")
    el_anio_que_viene = hoy.replace(year=hoy.year + 1).strftime("%Y-%m")

    ruta = ['Barcelona', 'Tokio', 'Paris', 'Nueva York']

    print(f"\n📅 BUSCANDO MEJORES PRECIOS PARA EL AÑO 2025:")
    informe_anual = optimizer.optimize_route(ruta, "2025")
    for ciudad, info in informe_anual.items():
        print(f"- {ciudad}: {info}")

    print(f"\n🚀 BUSCANDO PARA EL MES PRÓXIMO ({proximo_mes}):")
    informe_mensual = optimizer.optimize_route(ruta, proximo_mes)
    for ciudad, info in informe_mensual.items():
        print(f"- {ciudad}: {info}")