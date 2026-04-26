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
        self.geolocator = Nominatim(user_agent="skyscanner_optimizer", timeout=10)

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
        """Obtiene el Entity ID detectando automáticamente el formato de respuesta."""
        url = f"{self.base_url}/autosuggest/flights"
        payload = {
            "query": {
                "searchTerm": city_name,
                "locale": self.config["locale"],
                "market": self.config["market"]
            }
        }
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Buscamos en todas las rutas posibles donde Skyscanner esconde los datos
                results = data.get("content", {}).get("results", {})
                places_data = results.get("places", data.get("places", []))
                
                # Convertimos a lista sea lo que sea que venga (dict o list)
                items = places_data.values() if isinstance(places_data, dict) else places_data
                
                if not items: return None

                # 1. Intentamos buscar una ciudad o aeropuerto exacto
                for p in items:
                    p_type = p.get("type")
                    if p_type in ["PLACE_TYPE_CITY", "PLACE_TYPE_AIRPORT"]:
                        return p.get("entityId")
                
                # 2. Si no hay nada claro, el primer ID que veamos
                for p in items:
                    eid = p.get("entityId")
                    if eid: return eid
        except Exception as e:
            print(f"⚠️ Error en autosuggest para {city_name}: {e}")
        return None

    def get_nearest_airports_fallback(self, city_name, radius_km=300):
        """Busca aeropuertos cercanos usando una base local de aeropuertos para mayor precisión."""
        import json
        import os
        
        location = self.geolocator.geocode(city_name, language=self.config["locale"][:2])
        if not location:
            print(f"❌ Geopy no encontró coordenadas para {city_name}")
            return None

        airport_file = "airports_cache.json"
        
        if not os.path.exists(airport_file):
            print("⏳ Descargando base de datos de aeropuertos para búsqueda global...")
            try:
                import requests
                r = requests.get("https://raw.githubusercontent.com/mwgg/Airports/master/airports.json", timeout=10)
                if r.status_code == 200:
                    with open(airport_file, "w") as f:
                        f.write(r.text)
            except Exception as e:
                print(f"⚠️ Error descargando aeropuertos: {e}")
                return None

        try:
            with open(airport_file, "r") as f:
                airports_db = json.load(f)
        except Exception as e:
            print(f"⚠️ Error leyendo aeropuertos: {e}")
            return None

        valid_airports = []
        for k, v in airports_db.items():
            if v.get("iata") and v.get("lat") and v.get("lon") and v.get("iata") != "\\N":
                dist = self.haversine(location.latitude, location.longitude, float(v["lat"]), float(v["lon"]))
                if dist <= radius_km:
                    valid_airports.append({
                        "iata": v["iata"],
                        "name": v["name"],
                        "distance": dist
                    })

        valid_airports.sort(key=lambda x: x["distance"])
        
        final_airports = []
        for ap in valid_airports[:5]:
            eid = self.get_city_entity(ap["iata"])
            if eid:
                final_airports.append({
                    "entityId": eid,
                    "name": f"{ap['name']} ({ap['iata']})",
                    "distance": ap["distance"]
                })
        
        return final_airports if final_airports else None

    def search_indicative_cheapest(self, origin_id, dest_id, year, month, day=1):
        """Busca el precio más barato usando datos de caché (Indicative)."""
        url = f"{self.base_url}/flights/indicative/search"
        payload = {
            "query": {
                "market": self.config["market"],
                "locale": self.config["locale"],
                "currency": self.config["currency"],
                "queryLegs": [{
                    "originPlace": {"queryPlace": {"entityId": origin_id}},
                    "destinationPlace": {"queryPlace": {"entityId": dest_id}},
                    "fixedDate": {"year": int(year), "month": int(month), "day": int(day)}
                }]
            }
        }
        try:
            res = requests.post(url, json=payload, headers=self.headers, timeout=10)
        except Exception:
            return "Error de conexión"
            
        if res.status_code == 200:
            data = res.json()
            quotes = data.get("content", {}).get("results", {}).get("quotes", {})
            
            if not quotes:
                return "Sin datos para este mes"
            
            best_quote = None
            min_p = float('inf')
            items = quotes.values() if isinstance(quotes, dict) else quotes
            for q in items:
                amt = q.get("minPrice", {}).get("amount")
                if amt and float(amt) < min_p:
                    min_p = float(amt)
                    best_quote = q
            
            if best_quote:
                outbound = best_quote.get("outboundLeg", {})
                return {
                    "price": min_p,
                    "departure": outbound.get("departureDateTime"),
                    "quote_at": outbound.get("quoteCreated"),
                    "is_direct": best_quote.get("isDirect", False),
                    "duration": "N/A",
                    "stops": 0 if best_quote.get("isDirect") else "1+"
                }
            return "Sin precios"
        return f"Error API ({res.status_code}: {res.text[:50]})"
    
    def _get_best_price(self, origin_id, dest_id, date):
        """Determina qué búsqueda realizar según el formato: YYYY, YYYY-MM o YYYY-MM-DD."""
        try:
            # Caso de fecha concreta (YYYY-MM-DD) usando API Indicativa
            dt = datetime.strptime(date, "%Y-%m-%d")
            print(f"   🔎 Fecha concreta detectada ({date}): Usando API INDICATIVA (Caché)")
            return self.search_indicative_cheapest(origin_id, dest_id, dt.year, dt.month, dt.day)
        except ValueError:
            pass 

        if len(date) == 7:  # Búsqueda mensual (YYYY-MM)
            print(f"   🔎 Mes detectado ({date}): Usando API INDICATIVA (Caché de Skyscanner)")
            year, month = date.split('-')
            return self.search_indicative_cheapest(origin_id, dest_id, year, month)
        
        elif len(date) == 4:  # Búsqueda anual (YYYY)
            print(f"   📅 Año detectado ({date}): Analizando próximos 6 meses con API INDICATIVA")
            results = []
            now = datetime.now()
            for i in range(6):
                # Calculamos el mes y año para cada uno de los próximos 6 meses
                month = (now.month + i - 1) % 12 + 1
                year = now.year + (now.month + i - 1) // 12
                p = self.search_indicative_cheapest(origin_id, dest_id, year, month)
                if isinstance(p, dict): results.append(p)
            return min(results, key=lambda x: x["price"]) if results else "Sin vuelos en los próximos meses"

        return "Formato de fecha no reconocido (usa YYYY, YYYY-MM o YYYY-MM-DD)"

    def _to_float(self, value, default=0.0):
        try:
            if value is None or value == "":
                return default
            return float(value)
        except Exception:
            return default

    def _destination_metadata(self, dest):
        """Normaliza destinos que pueden venir como string o como objeto JSON del análisis visual."""
        if isinstance(dest, dict):
            name = str(dest.get("name") or dest.get("city") or dest.get("destination_name") or "Destino").strip()
            city = str(dest.get("city") or name).strip()
            country = str(dest.get("country") or "").strip()
            address = str(dest.get("formatted_address") or "").strip()
            flight_search_city = str(
                dest.get("flight_search_city")
                or dest.get("flight_search_label")
                or dest.get("nearest_city")
                or city
                or name
            ).strip()
            lat = self._to_float(dest.get("latitude", dest.get("lat")), None)
            lon = self._to_float(dest.get("longitude", dest.get("lon")), None)
            coords = dest.get("coordinates") if isinstance(dest.get("coordinates"), dict) else {}
            if lat is None:
                lat = self._to_float(coords.get("latitude", coords.get("lat")), None)
            if lon is None:
                lon = self._to_float(coords.get("longitude", coords.get("lng")), None)
            return {
                "raw": dest,
                "display_name": name,
                "city": city,
                "country": country,
                "formatted_address": address,
                "flight_search_city": flight_search_city,
                "lat": lat,
                "lon": lon,
                "climate": dest.get("climate", ""),
                "landscape": dest.get("landscape", ""),
                "description": dest.get("description", ""),
                "maps_url": dest.get("maps_url"),
                "confidence": dest.get("confidence", dest.get("final_confidence")),
            }

        text = str(dest or "").strip()
        return {
            "raw": dest,
            "display_name": text,
            "city": text,
            "country": "",
            "formatted_address": "",
            "flight_search_city": text,
            "lat": None,
            "lon": None,
            "climate": "",
            "landscape": "",
            "description": "",
            "maps_url": None,
            "confidence": None,
        }

    def get_nearest_airports_from_coordinates(self, latitude, longitude, radius_km=3000):
        """Busca aeropuertos cercanos usando coordenadas ya conocidas del lugar visual."""
        lat = self._to_float(latitude, None)
        lon = self._to_float(longitude, None)
        if lat is None or lon is None:
            return None

        url = f"{self.base_url}/geo/hierarchy/flights/nearest"
        payload = {
            "locator": {"coordinates": {"latitude": lat, "longitude": lon}},
            "locale": self.config["locale"],
        }
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code != 200:
                return None
            data = response.json()
            results = data.get("content", {}).get("results", {})
            places_data = results.get("places", data.get("places", []))
            items = places_data.values() if isinstance(places_data, dict) else places_data

            valid_airports = []
            for p in items:
                if not isinstance(p, dict) or p.get("type") != "PLACE_TYPE_AIRPORT":
                    continue
                coords = p.get("coordinates", {}) or {}
                alat = coords.get("latitude")
                alon = coords.get("longitude")
                if alat is None or alon is None:
                    continue
                dist = self.haversine(lat, lon, alat, alon)
                if dist <= radius_km:
                    valid_airports.append({
                        "entityId": p.get("entityId"),
                        "name": p.get("name"),
                        "distance": dist,
                        "lat": alat,
                        "lon": alon,
                    })
            return sorted(valid_airports, key=lambda x: x["distance"])
        except Exception as e:
            print(f"⚠️ Error buscando aeropuertos por coordenadas: {e}")
            return None

    def optimize_route(self, cities, date):
        """
        Busca vuelos desde origen hacia destinos.

        `cities` puede contener strings o diccionarios procedentes del análisis visual.
        Antes fallaba con `unhashable type: 'dict'` porque se usaba el diccionario completo como
        clave de `final_data['results']`. Ahora normalizamos cada destino y usamos siempre una clave string.
        """
        origin = str(cities[0] if cities else "Barcelona")
        destinations = cities[1:]

        try:
            origin_loc = self.geolocator.geocode(origin, language=self.config["locale"][:2])
        except Exception:
            origin_loc = None

        origin_id = self.get_city_entity(origin)
        if not origin_id:
            print(f" Origen {origin} no reconocido. Buscando aeropuertos cercanos...")
            nearby = self.get_nearest_airports_fallback(origin)
            if nearby:
                origin_id = nearby[0]["entityId"]
                print(f"✅ Usando aeropuerto cercano: {nearby[0]['name']}")
            else:
                raise ValueError(f"No se pudo localizar el origen '{origin}' ni aeropuertos cercanos.")

        final_data = {
            "origin": {
                "name": origin,
                "lat": origin_loc.latitude if origin_loc else 0,
                "lon": origin_loc.longitude if origin_loc else 0,
            },
            "results": {},
        }

        for dest in destinations:
            meta = self._destination_metadata(dest)
            display_name = meta["display_name"] or meta["flight_search_city"] or "Destino"
            flight_search_city = meta["flight_search_city"] or display_name
            print(f"🔎 Buscando vuelos para: {display_name} → destino aéreo: {flight_search_city}...")

            map_lat = meta["lat"]
            map_lon = meta["lon"]

            dest_loc = None
            if map_lat is None or map_lon is None:
                try:
                    dest_loc = self.geolocator.geocode(flight_search_city, language=self.config["locale"][:2])
                    if dest_loc:
                        map_lat = dest_loc.latitude
                        map_lon = dest_loc.longitude
                except Exception:
                    dest_loc = None

            dest_id = self.get_city_entity(flight_search_city)

            flight_info = None
            via_airport = None
            airport_distance_km = None

            if not dest_id:
                nearby = None
                if meta["lat"] is not None and meta["lon"] is not None:
                    print(f"   ⚠️ No hay entityId para {flight_search_city}. Buscando aeropuerto cercano por coordenadas...")
                    nearby = self.get_nearest_airports_from_coordinates(meta["lat"], meta["lon"])
                if not nearby:
                    print(f"   ⚠️ Fallback por nombre para {flight_search_city}...")
                    nearby = self.get_nearest_airports_fallback(flight_search_city)

                if nearby:
                    for airport in nearby[:3]:
                        res = self._get_best_price(origin_id, airport["entityId"], date)
                        if isinstance(res, dict):
                            flight_info = res
                            via_airport = airport.get("name")
                            airport_distance_km = airport.get("distance")
                            break
            else:
                flight_info = self._get_best_price(origin_id, dest_id, date)

            base_result = {
                "destination_name": display_name,
                "flight_search_city": flight_search_city,
                "country": meta.get("country", ""),
                "formatted_address": meta.get("formatted_address", ""),
                "visual_lat": meta["lat"],
                "visual_lon": meta["lon"],
                "lat": map_lat if map_lat is not None else 0,
                "lon": map_lon if map_lon is not None else 0,
                "via_airport": via_airport,
                "airport_distance_km": round(airport_distance_km, 1) if airport_distance_km is not None else None,
                "maps_url": meta.get("maps_url"),
                "confidence": meta.get("confidence"),
            }

            if isinstance(flight_info, dict):
                dep = flight_info.get("departure", {})
                date_str = "N/A"
                if isinstance(dep, dict):
                    y, m, d = dep.get("year"), dep.get("month"), dep.get("day")
                    if y and m and d:
                        date_str = f"{y}-{int(m):02d}-{int(d):02d}"

                obs_raw = flight_info.get("quote_at")
                obs_str = "N/A"
                if obs_raw:
                    try:
                        obs_dt = datetime.fromisoformat(obs_raw.replace("Z", "+00:00"))
                        obs_str = obs_dt.strftime("%d/%m %H:%M")
                    except Exception:
                        obs_str = "Reciente"

                price_int = int(float(flight_info["price"]))
                price_val = f"{price_int} €"
                if via_airport:
                    price_val += f" (vía {via_airport})"

                is_direct = flight_info.get("is_direct", False)
                stops_label = "Directo" if is_direct else "1 o más escalas"
                base_result.update({
                    "price": price_val,
                    "date": date_str,
                    "observed": obs_str,
                    "duration": f"{flight_info['duration']} min" if flight_info.get("duration") and flight_info["duration"] != "N/A" else None,
                    "stops": stops_label,
                })
            else:
                base_result.update({
                    "price": str(flight_info) if flight_info else "Sin resultados",
                    "date": "N/A",
                    "observed": "N/A",
                    "duration": None,
                    "stops": "N/A",
                })

            key = str(display_name)
            final_data["results"][key] = base_result

        return final_data

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
    for ciudad, info in informe_anual["results"].items():
        print(f"- {ciudad}: {info}")

    print(f"\n🚀 BUSCANDO PARA EL MES PRÓXIMO ({proximo_mes}):")
    informe_mensual = optimizer.optimize_route(ruta, proximo_mes)
    for ciudad, info in informe_mensual["results"].items():
        print(f"- {ciudad}: {info}")