import os
from flights import SkyscannerOptimizer

class Optimo(SkyscannerOptimizer):
    def get_nearest_airports_fallback(self, city_name, radius_km=300):
        """Busca aeropuertos cercanos con soporte para múltiples formatos de JSON."""
        import json
        import os
        
        location = self.geolocator.geocode(city_name, language=self.config["locale"][:2])
        if not location:
            print(f"❌ Geopy no encontró coordenadas para {city_name}")
            return None

        # Fallback mejorado: Usar base de datos de aeropuertos para encontrar todos los cercanos
        airport_file = "airports_cache.json"
        
        # Descargar caché si no existe
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
        except:
            return None

        valid_airports = []
        for k, v in airports_db.items():
            if v.get("iata") and v.get("lat") and v.get("lon") and v.get("iata") != "\\N":
                dist = self.haversine(location.latitude, location.longitude, v["lat"], v["lon"])
                if dist <= radius_km:
                    valid_airports.append({
                        "iata": v["iata"],
                        "name": v["name"],
                        "distance": dist
                    })

        valid_airports.sort(key=lambda x: x["distance"])
        
        # Obtener los EntityIds reales usando la API de Skyscanner (hasta un máximo de 5 para no saturar)
        final_airports = []
        for ap in valid_airports[:5]:
            eid = self.get_city_entity(ap["iata"])
            if eid:
                final_airports.append({
                    "entityId": eid,
                    "name": ap["name"] + " (" + ap["iata"] + ")",
                    "distance": ap["distance"]
                })
        
        return final_airports if final_airports else None

optimizer = Optimo(os.getenv("SKYSCANNER_API_KEY"))
print("Igualada:", optimizer.get_nearest_airports_fallback("Igualada"))
print("Tremp:", optimizer.get_nearest_airports_fallback("Tremp"))
