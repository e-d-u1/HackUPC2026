# Skyscanner Route Optimizer - HackUPC 2026

Este proyecto es un optimizador de rutas de vuelo que utiliza la API de Skyscanner para encontrar los precios más baratos entre múltiples destinos. Permite búsquedas por fechas específicas, meses completos o incluso los mejores precios en una ventana de 12 meses (Rolling Year).

## 🚀 Requisitos Previos

*   Python 3.8 o superior.
*   Una clave de API de Skyscanner (Partners API).
*   Una clave de API de Pixabay (Gratuita, para fotos reales).

## 🛠️ Configuración del Proyecto

Sigue estos pasos para poner en marcha el proyecto en tu máquina local:

### 1. Crear un Entorno Virtual
Es recomendable usar un entorno virtual para mantener las dependencias aisladas.
```bash
python3 -m venv .venv
```

### 2. Activar el Entorno Virtual
*   **En Linux/macOS:**
    ```bash
    source .venv/bin/activate
    ```
*   **En Windows:**
    ```bash
    .venv\Scripts\activate
    ```

### 3. Instalar Dependencias
Instala todas las librerías necesarias ejecutando:
```bash
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno
Crea un archivo llamado `.env` en la raíz del proyecto y añade tu API Key:
```text
SKYSCANNER_API_KEY=tu_clave_aqui
PIXABAY_API_KEY=tu_clave_pixabay_aqui
```

## 💻 Ejecución

### Modo Script (Lógica de Vuelos)
Si quieres probar la lógica de optimización directamente en la consola:
```bash
python flights.py
```

### Modo API (Servidor FastAPI)
Para levantar el servidor web y acceder a la API:
```bash
python main.py
```
El servidor estará disponible en `http://localhost:8000`.

## 🌐 Endpoints de la API

*   **Búsqueda de Vuelos:** `GET /api/search`
    *   **Parámetros:**
        *   `origin`: Ciudad de origen (ej: Murcia).
        *   `destinations`: Lista separada por comas (ej: Tokio,Paris).
        *   `date`: Puede ser un año (`2025`) o un mes (`2025-06`).
    *   **Ejemplo:** `http://localhost:8000/api/search?origin=Madrid&destinations=Londres,Roma&date=2025-10`

## 📁 Estructura del Proyecto
*   `flights.py`: Contiene la clase `SkyscannerOptimizer` con la lógica de negocio y fallback de aeropuertos.
*   `main.py`: Punto de entrada de la aplicación FastAPI.
*   `requirements.txt`: Lista de librerías de Python necesarias.
*   `.env`: Archivo para credenciales sensibles (no incluido en Git).
