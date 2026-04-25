from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os
from dotenv import load_dotenv
from flights import SkyscannerOptimizer

load_dotenv()
app = FastAPI()
# Asegúrate de crear una carpeta 'templates' y meter el index.html ahí
templates = Jinja2Templates(directory="templates")

API_KEY = os.getenv("SKYSCANNER_API_KEY")
optimizer = SkyscannerOptimizer(API_KEY)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/api/search")
async def api_search(origin: str, destinations: str, date: str = "2025"): # Corregido: Eliminada la línea duplicada
    dest_list = destinations.split(",")
    # Usamos la lógica de optimización que ya tienes en flights.py, pasando el parámetro 'date'
    results = optimizer.optimize_route([origin] + [d.strip() for d in dest_list], date) # Corregido: Eliminada la línea duplicada
    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)