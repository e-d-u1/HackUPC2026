from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os
import requests
from dotenv import load_dotenv
from flights import SkyscannerOptimizer
from hotels import HotelSearcher
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
app = FastAPI()
# Asegúrate de crear una carpeta 'templates' y meter el index.html ahí
templates = Jinja2Templates(directory="templates")

API_KEY = os.getenv("SKYSCANNER_API_KEY")
if not API_KEY:
    print("⚠️ ADVERTENCIA: No se encontró SKYSCANNER_API_KEY en el archivo .env")
optimizer = SkyscannerOptimizer(API_KEY)
hotel_searcher = HotelSearcher(API_KEY)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/api/search")
def api_search(origin: str, destinations: str, date: str = "2025"):
    """Ruta síncrona para manejar peticiones bloqueantes de red de forma eficiente."""
    dest_list = destinations.split(",")
    try:
        print(f"🚀 Iniciando optimización para {origin} -> {destinations} ({date})")
        results = optimizer.optimize_route([origin] + [d.strip() for d in dest_list], date)
        
        # Inyectamos el precio del hotel (que es rápido)
        for dest_name in results["results"]:
            results["results"][dest_name]["hotel_price"] = hotel_searcher.get_hotel_prices(dest_name)
            
        return results
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"❌ ERROR EN API: {str(e)}") # Esto saldrá en tu terminal
        raise HTTPException(status_code=500, detail=f"Error en el servidor: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7999)